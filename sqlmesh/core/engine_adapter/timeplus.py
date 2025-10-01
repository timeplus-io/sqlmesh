from __future__ import annotations

import typing as t
import logging
from sqlglot import exp
from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport
from sqlmesh.core.engine_adapter.mixins import LogicalMergeMixin
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    CommentCreationView,
    CommentCreationTable,
    CatalogSupport,
    SourceQuery,
    InsertOverwriteStrategy,
    EngineRunMode,
)
from sqlmesh.core.schema_diff import TableAlterOperation
from sqlmesh.utils import get_source_columns_to_types

if t.TYPE_CHECKING:
    import pandas as pd
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query, QueryOrDF
    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


class TimeplusEngineAdapter(EngineAdapterWithIndexSupport, LogicalMergeMixin):
    """
    Engine adapter for Timeplus/Proton streaming database.

    Timeplus is a streaming database optimized for real-time analytics
    with streams instead of tables.
    """

    DIALECT = "timeplus"  # Use native Timeplus dialect for streaming features
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_VIEW_SCHEMA = False
    SUPPORTS_REPLACE_TABLE = False
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    CATALOG_SUPPORT = CatalogSupport.SINGLE_CATALOG_ONLY
    # Timeplus materialized views require target_stream specification
    SUPPORTS_MATERIALIZED_VIEWS = True
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INTO_IS_OVERWRITE
    # Enable view binding to allow versioned table references in views
    # This allows SQLMesh virtual environments to work with Timeplus streaming
    # External streams (Kafka, etc.) should use kind EXTERNAL to avoid versioning
    HAS_VIEW_BINDING = True

    DEFAULT_BATCH_SIZE = 10000
    SCHEMA_DIFFER_KWARGS = {}
    DEFAULT_TABLE_ENGINE = "Stream"  # Timeplus uses streams instead of MergeTree

    # Timeplus handles clustering transparently through replication_factor setting
    # Example: replication_factor=3 ensures each node in a 3-node cluster has a full replica
    # This is user-controlled and managed by the Timeplus backend DBMS
    @property
    def cluster(self) -> t.Optional[str]:
        # Timeplus doesn't use ON CLUSTER syntax, clustering is handled via replication_factor
        return None

    @property
    def engine_run_mode(self) -> EngineRunMode:
        if self._extra_config.get("cloud_mode"):
            return EngineRunMode.CLOUD
        # Timeplus doesn't expose cluster mode - it's handled transparently
        return EngineRunMode.STANDALONE

    def fetchone(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.Tuple:
        """Fetch a single row from the query result.

        Note: In Timeplus, queries on streams will read from the streaming store
        by default. Use table() function to read from historical store:
        - SELECT * FROM stream - reads from streaming store (may wait for new data)
        - SELECT * FROM table(stream) - reads from historical store (bounded)
        """
        with self.transaction():
            self.execute(
                query,
                ignore_unsupported_errors=ignore_unsupported_errors,
                quote_identifiers=quote_identifiers,
            )
            rows = self.cursor.fetchall()
            return rows[0] if rows else ()

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        """Fetches a Pandas DataFrame from the cursor.

        Note: In Timeplus, queries on streams will read from the streaming store.
        Use table() function for bounded historical queries.
        """
        return self.cursor.client.query_df(
            self._to_sql(query, quote=quote_identifiers)
            if isinstance(query, exp.Expression)
            else query,
            use_extended_dtypes=True,
        )

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given database.

        Maps Timeplus engine types to SQLMesh DataObjectType:
        - View, MaterializedView -> view
        - Stream, ExternalStream, ExternalTable -> table
        """
        from sqlmesh.core.dialect import to_schema

        query = (
            exp.select(
                exp.column("database").as_("schema_name"),
                exp.column("name"),
                exp.case(exp.column("engine"))
                .when(
                    exp.Literal.string("View"),
                    exp.Literal.string("view"),
                )
                .when(
                    exp.Literal.string("MaterializedView"),
                    exp.Literal.string("view"),
                )
                .else_(
                    # Map Stream, ExternalStream, ExternalTable to table
                    exp.Literal.string("table"),
                )
                .as_("type"),
            )
            .from_("system.tables")
            .where(exp.column("database").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=None,
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(str(row.type)),
            )
            for row in df.itertuples()
        ]

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.List[exp.Expression] = [],
    ) -> None:
        """Create a Timeplus database from a name or qualified table name."""
        self._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties,
            kind="DATABASE",
        )

    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        track_rows_processed: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """Creates a stream in Timeplus instead of a table."""
        logger.info(
            f"_create_table called: table_name_or_schema={table_name_or_schema}, expression_type={type(expression)}, table_kind={table_kind}"
        )

        # Views remain as views
        if table_kind == "VIEW":
            super()._create_table(
                table_name_or_schema,
                expression,
                exists,
                replace,
                target_columns_to_types,
                table_description,
                column_descriptions,
                table_kind,
                track_rows_processed,
                **kwargs,
            )
            return

        # Check if this is an external stream or table
        stream_table_props = kwargs.get("table_properties", {})
        external_type = stream_table_props.get("external_type")

        if external_type:
            # Create external stream or table
            self._create_external_object(
                table_name_or_schema=table_name_or_schema,
                external_type=external_type,
                target_columns_to_types=target_columns_to_types,
                exists=exists,
                replace=replace,
                table_properties=stream_table_props,
                track_rows_processed=track_rows_processed,
            )
            return

        # Determine stream type based on properties
        is_mutable = False
        is_changelog = False
        primary_key = None

        if isinstance(table_name_or_schema, exp.Schema):
            # Check for primary key in schema definition
            for expr in table_name_or_schema.expressions:
                if isinstance(expr, exp.PrimaryKey):
                    is_mutable = True
                    primary_key = expr
                    break

        # Check if this is a changelog_kv or versioned_kv stream
        stream_table_props = kwargs.get("table_properties", {})
        if stream_table_props and stream_table_props.get("mode") in (
            "changelog_kv",
            "versioned_kv",
        ):
            is_changelog = True
            is_mutable = True  # changelog_kv is a type of mutable stream

        # Build CREATE STREAM statement
        if isinstance(table_name_or_schema, exp.Schema):
            schema = table_name_or_schema
            table_name = schema.this
        else:
            table_name = table_name_or_schema
            # Build schema from target_columns_to_types if provided
            if target_columns_to_types:
                column_defs = []
                for col_name, col_type in target_columns_to_types.items():
                    column_defs.append(
                        exp.ColumnDef(
                            this=exp.to_column(col_name),
                            kind=col_type,
                        )
                    )
                schema = exp.Schema(
                    this=exp.to_table(table_name),
                    expressions=column_defs,
                )
            else:
                schema = None

        # Timeplus doesn't support CREATE OR REPLACE, handle replacement differently
        if replace:
            # Drop the existing stream first if replacing
            self.drop_table(table_name, ignore_if_not_exists=True)
            exists = False  # Don't use IF NOT EXISTS after dropping

        # Create the stream creation expression
        if is_changelog:
            stream_kind = "STREAM"  # changelog_kv uses CREATE STREAM with mode setting
        else:
            stream_kind = "MUTABLE STREAM" if is_mutable else "STREAM"

        create_expr = exp.Create(
            this=exp.to_table(table_name) if not schema else schema,
            kind=stream_kind,
            exists=exists,
            replace=False,  # Never use CREATE OR REPLACE for Timeplus
        )

        # For non-external streams, always create empty streams from schema
        # External streams/tables are handled separately above
        # Regular streams can't execute queries with CAST(NULL...) in Timeplus
        logger.info(f"Creating regular stream {table_name} with schema from columns_to_types")
        # Don't set expression - create empty stream with just schema

        # Add properties
        # Remove partitioned_by and table_properties from kwargs to avoid duplication
        kwargs_copy = kwargs.copy()
        kwargs_copy.pop("partitioned_by", None)
        kwargs_copy.pop("table_properties", None)

        properties = self._build_stream_properties_exp(
            is_mutable=is_mutable,
            is_changelog=is_changelog,
            primary_key=primary_key,
            partitioned_by=kwargs.get("partitioned_by"),
            table_properties=kwargs.get("table_properties"),
            target_columns_to_types=target_columns_to_types,
            table_description=table_description,
            **kwargs_copy,
        )

        if properties:
            create_expr.set("properties", properties)

        self.execute(create_expr, track_rows_processed=track_rows_processed)

    def _create_external_object(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        external_type: str,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        table_properties: t.Optional[t.Dict[str, t.Any]] = None,
        track_rows_processed: bool = True,
    ) -> None:
        """Create an external stream or external table for integrations.

        Args:
            table_name_or_schema: Table name or schema definition
            external_type: Type of external object (kafka, clickhouse, etc.)
            target_columns_to_types: Column definitions (optional for external tables)
            exists: Whether to use IF NOT EXISTS
            replace: Whether to drop and recreate
            table_properties: Properties including connection settings
            track_rows_processed: Whether to track rows
        """
        # Extract table name
        if isinstance(table_name_or_schema, exp.Schema):
            table_name = table_name_or_schema.this
            schema = table_name_or_schema
        else:
            table_name = table_name_or_schema
            schema = None

        # Handle replace by dropping first
        if replace:
            self.drop_table(table_name, ignore_if_not_exists=True)
            exists = False

        # Determine if this is a stream or table based on external type
        # Kafka and similar sources are streams, ClickHouse and similar sinks are tables
        # external_type might be a SQLGlot expression, convert to string
        external_type_str = (
            str(external_type).strip("'\"").lower()
            if not isinstance(external_type, str)
            else external_type.lower()
        )
        is_external_table = external_type_str in ["clickhouse", "postgresql", "mysql"]

        object_kind = "EXTERNAL TABLE" if is_external_table else "EXTERNAL STREAM"

        # Build column definitions if provided
        columns_sql = ""
        if target_columns_to_types and not is_external_table:
            # External streams need column definitions
            # External tables don't (they infer from remote table)
            col_defs = []
            for col_name, col_type in target_columns_to_types.items():
                col_defs.append(
                    f"{exp.to_column(col_name).sql(dialect=self.dialect)} {col_type.sql(dialect=self.dialect)}"
                )
            columns_sql = f"({', '.join(col_defs)})"
        # Note: For external tables (ClickHouse, PostgreSQL, MySQL), we don't include column definitions
        # The schema is inferred from the remote table

        # Build SETTINGS clause
        settings = self._build_external_settings(external_type_str, table_properties or {})

        # Construct the CREATE statement
        table_sql = exp.to_table(table_name).sql(dialect=self.dialect)
        exists_clause = "IF NOT EXISTS " if exists else ""

        create_sql = f"CREATE {object_kind} {exists_clause}{table_sql}"
        if columns_sql:
            create_sql += f" {columns_sql}"
        if settings:
            create_sql += f"\nSETTINGS {settings}"

        logger.info(f"Creating Timeplus {object_kind}: {table_name}")
        self.execute(create_sql, track_rows_processed=track_rows_processed)

    def _build_external_settings(
        self, external_type: str, table_properties: t.Dict[str, t.Any]
    ) -> str:
        """Build SETTINGS clause for external stream/table.

        Args:
            external_type: Type of external object (kafka, clickhouse, etc.)
            table_properties: Properties dict containing connection settings

        Returns:
            Formatted SETTINGS string
        """
        settings_parts = []

        # Always add type first
        settings_parts.append(f"type = '{external_type}'")

        # Known settings for different external types
        KAFKA_SETTINGS = {
            "brokers",
            "topic",
            "data_format",
            "consumer_group",
            "sasl_mechanism",
            "sasl_username",
            "sasl_password",
            "security_protocol",
            "ssl_ca_cert_file",
            "auto_offset_reset",
        }

        CLICKHOUSE_SETTINGS = {
            "address",
            "database",
            "table",
            "user",
            "password",
            "secure",
        }

        POSTGRES_MYSQL_SETTINGS = {
            "host",
            "port",
            "database",
            "table",
            "user",
            "password",
        }

        # Determine which settings to look for
        if external_type.lower() == "kafka":
            relevant_settings = KAFKA_SETTINGS
        elif external_type.lower() == "clickhouse":
            relevant_settings = CLICKHOUSE_SETTINGS
        elif external_type.lower() in ["postgresql", "mysql"]:
            relevant_settings = POSTGRES_MYSQL_SETTINGS
        else:
            # Unknown type, try to pass through all non-external_type settings
            relevant_settings = set()

        # Add relevant settings from table_properties
        for key, value in table_properties.items():
            if key == "external_type":
                continue  # Already handled

            # Check if this is a relevant setting or if we're in pass-through mode
            if relevant_settings and key not in relevant_settings:
                continue

            # Format value based on type
            # value might be a SQLGlot expression, convert to raw value
            if isinstance(value, exp.Expression):
                if isinstance(value, exp.Literal):
                    raw_value = value.this
                else:
                    raw_value = str(value).strip("'\"")
            else:
                raw_value = value

            if isinstance(raw_value, bool):
                formatted_value = "true" if raw_value else "false"
            elif isinstance(raw_value, (int, float)):
                formatted_value = str(raw_value)
            else:
                # String value, quote it
                formatted_value = f"'{raw_value}'"

            settings_parts.append(f"{key} = {formatted_value}")

        return ", ".join(settings_parts)

    def _build_stream_properties_exp(
        self,
        is_mutable: bool = False,
        is_changelog: bool = False,
        primary_key: t.Optional[exp.PrimaryKey] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Build properties for Timeplus streams."""

        properties: t.List[exp.Expression] = []

        # Copy table properties to handle Timeplus settings
        settings = table_properties.copy() if table_properties else {}

        # Handle replication_factor if specified
        replication_factor = settings.pop("replication_factor", kwargs.get("replication_factor"))

        # For mutable streams, partitioning is not supported
        if is_mutable and partitioned_by:
            logger.warning(
                "Mutable streams do not support partitioning. Ignoring PARTITION BY clause."
            )
            partitioned_by = None

        # Add ORDER BY for append streams (MergeTree backend)
        if not is_mutable:
            ordered_by = settings.pop("ORDER_BY", None)
            if ordered_by:
                if isinstance(ordered_by, (list, tuple)):
                    ordered_by_exprs = [
                        col if isinstance(col, exp.Expression) else exp.column(col)
                        for col in ordered_by
                    ]
                else:
                    if isinstance(ordered_by, exp.Expression):
                        ordered_by_exprs = [ordered_by]
                    else:
                        ordered_by_exprs = [exp.column(str(ordered_by))]
                properties.append(exp.Order(expressions=[exp.Tuple(expressions=ordered_by_exprs)]))
            elif not partitioned_by:
                # Default empty ORDER BY for non-partitioned append streams
                properties.append(exp.Order(expressions=[exp.Tuple(expressions=[])]))

        # Add PRIMARY KEY for mutable streams (already part of schema)
        if is_mutable and primary_key and not isinstance(primary_key, exp.PrimaryKey):
            # If primary_key is not already an exp.PrimaryKey, create it
            if isinstance(primary_key, (list, tuple)):
                pk_cols = [exp.column(col) if isinstance(col, str) else col for col in primary_key]
            else:
                pk_cols = (
                    [primary_key]
                    if isinstance(primary_key, exp.Expression)
                    else [exp.column(primary_key)]
                )
            properties.append(exp.PrimaryKey(expressions=pk_cols))

        # Add partitioning for append streams only
        if partitioned_by and not is_mutable:
            properties.append(
                exp.PartitionedByProperty(this=exp.Schema(expressions=partitioned_by))
            )

        # Build SETTINGS clause
        settings_expressions = []

        # Add replication_factor if specified
        if replication_factor:
            settings_expressions.append(
                exp.EQ(
                    this=exp.var("replication_factor"),
                    expression=exp.Literal.number(str(replication_factor)),
                )
            )

        # Handle stream mode for special stream types
        # Only versioned_kv, changelog_kv, and changelog modes exist in Timeplus
        # Regular streams don't have any mode setting
        stream_mode = settings.pop("mode", None) if settings else None
        if stream_mode:
            # Only set mode for special stream types (versioned_kv, changelog_kv, changelog)
            settings_expressions.append(
                exp.EQ(this=exp.var("mode"), expression=exp.Literal.string(stream_mode))
            )
        elif is_changelog:
            # Changelog streams need explicit mode setting
            settings_expressions.append(
                exp.EQ(this=exp.var("mode"), expression=exp.Literal.string("changelog_kv"))
            )

        # Add remaining settings
        for key, value in settings.items():
            settings_expressions.append(
                exp.EQ(
                    this=exp.var(key.lower()),
                    expression=value
                    if isinstance(value, exp.Expression)
                    else exp.Literal.string(str(value)),
                )
            )

        if settings_expressions:
            properties.append(exp.SettingsProperty(expressions=settings_expressions))

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        return exp.Properties(expressions=properties) if properties else None

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        empty_ctas: bool = False,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Override to use stream properties instead.

        Note: Filters out materialized view-specific properties that don't apply to streams/tables.
        """
        # Filter out MV-specific properties that shouldn't be in stream/table creation
        MV_ONLY_PROPERTIES = {
            "checkpoint_interval",
            "checkpoint_settings",
            "pause_on_start",
            "memory_weight",
            "preferred_exec_node",
            "default_hash_table",
            "default_hash_join",
            "target_stream",  # MV INTO clause target
        }

        # Remove MV properties from table_properties if present
        if table_properties:
            table_properties = {
                k: v for k, v in table_properties.items() if k not in MV_ONLY_PROPERTIES
            }

        # Filter MV properties from kwargs
        filtered_kwargs = {k: v for k, v in kwargs.items() if k not in MV_ONLY_PROPERTIES}

        return self._build_stream_properties_exp(
            is_mutable=False,
            is_changelog=False,
            primary_key=None,
            partitioned_by=partitioned_by,
            table_properties=table_properties,
            target_columns_to_types=target_columns_to_types,
            table_description=table_description,
            **filtered_kwargs,
        )

    def create_view(
        self,
        view_name: TableName,
        query_or_df: t.Any,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        """Create a view in Timeplus.

        For materialized views, requires 'target_stream' in materialized_properties.

        Example:
            materialized_properties = {
                'target_stream': 'output_stream',
                'checkpoint_interval': 60
            }
        """
        if materialized:
            # For SQLMesh models, physical_properties are passed as view_properties
            # We need to merge them with materialized_properties for Timeplus materialized views
            if not materialized_properties:
                materialized_properties = {}

            # Merge view_properties into materialized_properties if present
            if view_properties:
                # Convert view_properties expressions to values for materialized_properties
                for key, value in view_properties.items():
                    # Convert expression to appropriate Python value
                    if isinstance(value, exp.Expression):
                        # Handle different expression types
                        if isinstance(value, exp.Literal):
                            # Extract literal value
                            if value.is_string:
                                # Check if it's a boolean string
                                if value.this.lower() in ("true", "false"):
                                    materialized_properties[key] = value.this.lower() == "true"
                                else:
                                    materialized_properties[key] = value.this
                            elif value.is_int:
                                materialized_properties[key] = int(value.this)
                            else:
                                materialized_properties[key] = value.this
                        elif isinstance(value, exp.Boolean):
                            materialized_properties[key] = value.this
                        elif isinstance(value, exp.Identifier):
                            materialized_properties[key] = value.this
                        else:
                            # For other expressions, try to get string representation
                            materialized_properties[key] = value.sql(dialect="timeplus")
                    else:
                        # If not an expression, use value directly
                        materialized_properties[key] = value

            # Check for target_stream
            if "target_stream" not in materialized_properties:
                raise ValueError(
                    "Timeplus MATERIALIZED VIEWs require 'target_stream' in physical_properties. "
                    "Example:\n"
                    "MODEL (\n"
                    "    name my_model,\n"
                    "    kind VIEW (\n"
                    "        materialized true\n"
                    "    ),\n"
                    "    physical_properties (\n"
                    "        target_stream = 'output_stream',\n"
                    "        checkpoint_interval = 60\n"
                    "    )\n"
                    ");"
                )

            # Create the materialized view with INTO clause
            self._create_materialized_view_with_target(
                view_name=view_name,
                query_or_df=query_or_df,
                target_stream=materialized_properties["target_stream"],
                materialized_properties=materialized_properties,
                replace=replace,
                **create_kwargs,
            )
            return

        super().create_view(
            view_name=view_name,
            query_or_df=query_or_df,
            target_columns_to_types=target_columns_to_types,
            replace=replace,
            materialized=False,  # Always create regular views
            materialized_properties=None,  # Not applicable for regular views
            table_description=table_description,
            column_descriptions=column_descriptions,
            view_properties=view_properties,
            source_columns=source_columns,
            **create_kwargs,
        )

    def _create_materialized_view_with_target(
        self,
        view_name: TableName,
        query_or_df: t.Any,
        target_stream: str,
        materialized_properties: t.Dict[str, t.Any],
        replace: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """Create a Timeplus materialized view with INTO clause.

        Args:
            view_name: Name of the materialized view to create
            query_or_df: Query or dataframe for the view
            target_stream: Target stream name for INTO clause
            materialized_properties: Additional properties like checkpoint_interval
            replace: Whether to replace existing view

        Note:
            Ignores table-level kwargs like partitioned_by, clustered_by since
            materialized views in Timeplus don't support table partitioning.
        """

        # Filter out table-level properties that don't apply to materialized views
        # These come from SQLMesh's model defaults but aren't valid for Timeplus MVs
        _ignored_kwargs = {
            "partitioned_by",
            "clustered_by",
            "partition_interval_unit",
            "table_properties",
            "table_format",
            "storage_format",
        }
        logger.debug(
            f"Creating materialized view '{view_name}', ignoring kwargs: {set(kwargs.keys()) & _ignored_kwargs}"
        )

        # Convert query to expression if needed
        if isinstance(query_or_df, str):
            from sqlglot import parse_one

            query_expr = parse_one(query_or_df, dialect=self.dialect)
        elif isinstance(query_or_df, exp.Expression):
            query_expr = query_or_df
        else:
            raise ValueError("Materialized views require a query, not a DataFrame")

        # Check that target stream exists - user must create it explicitly
        target_table = exp.to_table(target_stream)
        if not self.table_exists(target_table):
            raise ValueError(
                f"Target stream '{target_stream}' does not exist. "
                f"Please create it explicitly before creating the materialized view. "
                f"Example:\n"
                f"  CREATE STREAM {target_stream} (col1 type1, col2 type2, ...);"
            )

        # Build the CREATE MATERIALIZED VIEW statement
        view_table = exp.to_table(view_name)
        target_table = exp.to_table(target_stream)

        # Timeplus doesn't support CREATE OR REPLACE, use drop+create when replace=True
        if replace:
            self.drop_view(view_name, ignore_if_not_exists=True)

        # Always use CREATE ... IF NOT EXISTS for safety
        # Build CREATE MATERIALIZED VIEW ... INTO ... AS SELECT ...
        create_sql = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {view_table.sql(dialect=self.dialect)}
INTO {target_table.sql(dialect=self.dialect)}
AS {query_expr.sql(dialect=self.dialect)}"""

        # Add SETTINGS if provided
        # Support all Timeplus MV settings
        KNOWN_MV_SETTINGS = {
            "checkpoint_interval",
            "checkpoint_settings",  # Complex checkpoint configuration
            "pause_on_start",  # Start MV in paused state
            "memory_weight",  # For cluster load balancing
            "preferred_exec_node",  # Explicit node assignment
            "default_hash_table",  # Hash table type (hybrid/linear)
            "default_hash_join",  # Hash join type (hybrid/linear)
        }

        # Properties that should be ignored for materialized views (table-level only)
        TABLE_ONLY_PROPERTIES = {
            "partitioned_by",
            "clustered_by",
            "partition_interval_unit",
            "table_format",
            "storage_format",
            "empty_ctas",
        }

        settings = {}
        for key, value in materialized_properties.items():
            if key == "target_stream":
                continue  # Already handled in INTO clause

            if key in TABLE_ONLY_PROPERTIES:
                # Skip table-level properties that don't apply to materialized views
                logger.debug(f"Skipping table-level property '{key}' for materialized view")
                continue

            if key in KNOWN_MV_SETTINGS:
                # Handle different value types appropriately
                if key == "checkpoint_settings" and isinstance(value, str):
                    # checkpoint_settings is a string like 'incremental=true;type=rocks'
                    settings[key] = f"'{value}'"
                elif isinstance(value, bool):
                    settings[key] = "true" if value else "false"
                elif isinstance(value, str) and key in ["default_hash_table", "default_hash_join"]:
                    # These need to be quoted strings
                    settings[key] = f"'{value}'"
                else:
                    settings[key] = value
            else:
                # Log unknown settings but still pass them through for future compatibility
                logger.debug(f"Unknown materialized view setting '{key}' - passing through anyway")
                settings[key] = value

        if settings:
            settings_str = ", ".join(f"{k} = {v}" for k, v in settings.items())
            create_sql += f"\nSETTINGS {settings_str}"

        # Log the SQL before execution
        logger.info(f"Executing CREATE MATERIALIZED VIEW SQL:\n{create_sql}")

        # Execute the CREATE MATERIALIZED VIEW statement
        self.execute(create_sql)

        logger.info(
            f"Created Timeplus materialized view '{view_name}' streaming into '{target_stream}'"
        )

    def _build_view_properties_exp(
        self,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        table_description: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        properties: t.List[exp.Expression] = []

        view_properties_copy = view_properties.copy() if view_properties else {}

        if view_properties_copy:
            for key, value in view_properties_copy.items():
                properties.append(
                    exp.SettingsProperty(
                        expressions=[
                            exp.EQ(
                                this=exp.var(key.lower()),
                                expression=value
                                if isinstance(value, exp.Expression)
                                else exp.Literal.string(str(value)),
                            )
                        ]
                    )
                )

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        return exp.Properties(expressions=properties) if properties else None

    def alter_table(
        self,
        alter_expressions: t.Union[t.List[exp.Alter], t.List[TableAlterOperation]],
    ) -> None:
        """Performs alter statements on streams."""
        with self.transaction():
            for alter_expression in [
                x.expression if isinstance(x, TableAlterOperation) else x for x in alter_expressions
            ]:
                self.execute(alter_expression)

    def delete_from(self, table_name: TableName, where: t.Union[str, exp.Expression]) -> None:
        """Delete from stream.

        Note: For changelog_kv streams, DELETE works differently than regular streams.
        You need to insert a row with _tp_delta=-1 to mark it as deleted.
        """
        delete_expr = exp.delete(table_name, where)
        self.execute(delete_expr)

    def insert_overwrite_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expression],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> None:
        """Insert overwrite by partition for partitioned streams."""
        # For Timeplus, we can use ALTER TABLE ... DROP PARTITION and then INSERT
        table = exp.to_table(table_name)

        # Drop specified partitions first
        for partition in partitioned_by:
            drop_partition_sql = f"ALTER STREAM {table.sql(dialect=self.dialect)} DROP PARTITION {partition.sql(dialect=self.dialect)}"
            self.execute(drop_partition_sql)

        # Then insert new data
        self.insert_append(
            table_name, query_or_df, target_columns_to_types, source_columns=source_columns
        )

    def drop_view(
        self,
        view_name: TableName,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **drop_args: t.Any,
    ) -> None:
        """Drop a view or materialized view in Timeplus.

        Note: Timeplus uses DROP VIEW for both regular and materialized views.
        """
        # In Timeplus, both regular and materialized views are dropped with DROP VIEW
        self.execute(
            exp.Drop(
                this=exp.to_table(view_name),
                kind="VIEW",  # Always use VIEW, even for materialized views
                exists=ignore_if_not_exists,
                **drop_args,
            )
        )

    def rename_table(
        self,
        old_table: TableName,
        new_table: TableName,
        ignore_if_not_exists: bool = True,
    ) -> None:
        """Rename a stream."""
        old_table_exp = exp.to_table(old_table)
        new_table_exp = exp.to_table(new_table)

        # Timeplus uses RENAME STREAM syntax
        sql_str = f"RENAME STREAM{' IF EXISTS' if ignore_if_not_exists else ''} {old_table_exp.sql(dialect=self.dialect)} TO {new_table_exp.sql(dialect=self.dialect)}"
        self.execute(sql_str)

    def _drop_object(
        self,
        name: TableName | SchemaName,
        exists: bool = True,
        kind: str = "TABLE",
        cascade: bool = False,
        **drop_args: t.Any,
    ) -> None:
        """Drops an object (stream, database, view, etc.)."""
        # Convert TABLE to STREAM for drop operations
        if kind == "TABLE":
            kind = "STREAM"

        self.execute(
            exp.Drop(
                this=exp.to_table(name),
                kind=kind,
                exists=exists,
                cascade=cascade,
                **drop_args,
            )
        )

    def _truncate_table(self, table_name: TableName) -> None:
        """Truncate a stream by deleting all records."""
        # Timeplus doesn't support TRUNCATE, use DELETE instead
        self.execute(exp.Delete(this=exp.to_table(table_name)))

    def _create_table_like(
        self, target_table_name: TableName, source_table_name: TableName
    ) -> None:
        """Create stream with identical structure as source stream"""
        # Timeplus uses CREATE STREAM ... AS ... for structure copy
        self.execute(f"CREATE STREAM {target_table_name} AS {source_table_name}")

    def _df_to_source_queries(
        self,
        df: DF,
        target_columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
        source_columns: t.Optional[t.List[str]] = None,
        **kwargs: t.Any,
    ) -> t.List[SourceQuery]:
        """Convert DataFrame to source queries for bulk insert."""

        temp_table = self._get_temp_table(target_table, **kwargs)
        source_columns_to_types = get_source_columns_to_types(
            target_columns_to_types, source_columns
        )

        def query_factory() -> Query:
            if not self.table_exists(temp_table):
                self.create_table(
                    temp_table,
                    source_columns_to_types,
                    **kwargs,
                )
                ordered_df = df[list(source_columns_to_types)]

                # Use Timeplus client to insert DataFrame
                self.cursor.client.insert_df(temp_table.sql(dialect=self.dialect), df=ordered_df)

            return exp.select(*self._casted_columns(target_columns_to_types, source_columns)).from_(
                temp_table
            )

        return [
            SourceQuery(
                query_factory=query_factory,
                cleanup_func=lambda: self.drop_table(temp_table, **kwargs),
            )
        ]

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expression],
        **kwargs: t.Any,
    ) -> t.Optional[t.Union[exp.PartitionedByProperty, exp.Property]]:
        """Build PARTITION BY expression for append streams."""
        return exp.PartitionedByProperty(
            this=exp.Schema(expressions=partitioned_by),
        )

    def _build_create_comment_table_exp(
        self, table: exp.Table, table_comment: str, table_kind: str, **kwargs: t.Any
    ) -> exp.Comment | str:
        """Build comment expression for streams."""
        table_sql = table.sql(dialect=self.dialect, identify=True)

        # Convert TABLE to STREAM in ALTER statement
        stream_kind = "STREAM" if table_kind == "TABLE" else table_kind

        truncated_comment = self._truncate_table_comment(table_comment)
        comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

        return f"ALTER {stream_kind} {table_sql} MODIFY COMMENT {comment_sql}"

    def _build_create_comment_column_exp(
        self,
        table: exp.Table,
        column_name: str,
        column_comment: str,
        table_kind: str = "TABLE",
        **kwargs: t.Any,
    ) -> exp.Comment | str:
        """Build column comment expression for streams."""
        table_sql = table.sql(dialect=self.dialect, identify=True)
        column_sql = exp.to_column(column_name).sql(dialect=self.dialect, identify=True)

        # Convert TABLE to STREAM in ALTER statement
        stream_kind = "STREAM" if table_kind == "TABLE" else table_kind

        truncated_comment = self._truncate_table_comment(column_comment)
        comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

        return f"ALTER {stream_kind} {table_sql} COMMENT COLUMN {column_sql} {comment_sql}"
