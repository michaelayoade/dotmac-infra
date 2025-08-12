"""
Search SDK - Open Source PostgreSQL Full-Text Search Implementation
Uses PostgreSQL FTS, trigram extensions, and GIN indexes for fast search
No external dependencies - pure PostgreSQL solution
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class SearchQuery(BaseModel):
    """Search query parameters"""

    query: str = Field(..., min_length=1, description="Search query string")
    entity_types: List[str] = Field(
        default_factory=list, description="Entity types to search"
    )
    filters: Dict[str, Any] = Field(
        default_factory=dict, description="Additional filters"
    )
    limit: int = Field(default=20, ge=1, le=100, description="Maximum results")
    offset: int = Field(default=0, ge=0, description="Results offset for pagination")
    fuzzy: bool = Field(default=True, description="Enable fuzzy/trigram matching")


class SearchResult(BaseModel):
    """Individual search result"""

    entity_type: str
    entity_id: str
    score: float
    title: str
    description: str
    metadata: Dict[str, Any]
    highlighted_fields: Dict[str, str] = Field(default_factory=dict)


class SearchResults(BaseModel):
    """Search results container"""

    query: str
    total_count: int
    results: List[SearchResult]
    took_ms: int
    suggestions: List[str] = Field(default_factory=list)


class SearchableEntity(BaseModel):
    """Entity configuration for search indexing"""

    entity_type: str
    table_name: str
    id_column: str = "id"
    title_columns: List[str] = Field(default_factory=list)
    content_columns: List[str] = Field(default_factory=list)
    metadata_columns: List[str] = Field(default_factory=list)
    tenant_column: Optional[str] = "tenant_id"


class SearchClient:
    """
    Open-source search client using PostgreSQL Full-Text Search

    Features:
    - PostgreSQL FTS with tsvector/tsquery
    - Trigram similarity for fuzzy matching
    - GIN indexes for performance
    - Multi-tenant support
    - Configurable entity types
    - Auto-complete suggestions
    - Highlighted results
    """

    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id
        self.searchable_entities = self._get_searchable_entities()

    def _get_searchable_entities(self) -> Dict[str, SearchableEntity]:
        """Configure searchable entities"""
        return {
            "contact": SearchableEntity(
                entity_type="contact",
                table_name="contacts",
                title_columns=["first_name", "last_name", "company"],
                content_columns=[
                    "first_name",
                    "last_name",
                    "middle_name",
                    "title",
                    "company",
                    "department",
                ],
                metadata_columns=["contact_type", "created_at", "updated_at"],
            ),
            "address": SearchableEntity(
                entity_type="address",
                table_name="addresses",
                title_columns=["street_address", "city"],
                content_columns=[
                    "street_address",
                    "street_address_2",
                    "city",
                    "state_province",
                    "postal_code",
                    "country",
                ],
                metadata_columns=["address_type", "created_at"],
            ),
            "email": SearchableEntity(
                entity_type="email",
                table_name="emails",
                title_columns=["email_address"],
                content_columns=["email_address"],
                metadata_columns=["email_type", "is_verified", "is_primary"],
            ),
            "phone": SearchableEntity(
                entity_type="phone",
                table_name="phones",
                title_columns=["phone_number"],
                content_columns=["phone_number", "extension"],
                metadata_columns=["phone_type", "is_verified", "is_primary"],
            ),
            "organization": SearchableEntity(
                entity_type="organization",
                table_name="organizations",
                title_columns=["name"],
                content_columns=["name", "description", "industry", "tax_id"],
                metadata_columns=["organization_type", "created_at"],
            ),
            "customer": SearchableEntity(
                entity_type="customer",
                table_name="customers",
                title_columns=["portal_id"],
                content_columns=["portal_id"],
                metadata_columns=["customer_type", "status", "created_at"],
            ),
        }

    async def setup_search_infrastructure(self, db: Optional[Session]) -> bool:
        """
        Setup PostgreSQL search infrastructure. Safe no-op without DB.
        """
        if db is None:
            logger.info("Search infra setup skipped: no DB session provided")
            return True
        try:
            # Enable required PostgreSQL extensions
            await self._execute_sql(db, "CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            await self._execute_sql(db, "CREATE EXTENSION IF NOT EXISTS unaccent;")

            # Create search indexes for each entity
            for entity_config in self.searchable_entities.values():
                await self._create_search_index(db, entity_config)

            # Create search helper functions
            await self._create_search_functions(db)

            logger.info("Search infrastructure setup completed")
            return True
        except Exception as e:
            logger.error(f"Failed to setup search infrastructure: {str(e)}")
            return False

    async def _create_search_index(self, db: Session, entity: SearchableEntity) -> None:
        """Create FTS and trigram indexes for entity"""
        tsvector_sql = f"""
        ALTER TABLE {entity.table_name} 
        ADD COLUMN IF NOT EXISTS search_vector tsvector;
        """
        await self._execute_sql(db, tsvector_sql)

        gin_index_sql = f"""
        CREATE INDEX IF NOT EXISTS {entity.table_name}_search_vector_idx 
        ON {entity.table_name} USING GIN (search_vector);
        """
        await self._execute_sql(db, gin_index_sql)

        for column in entity.content_columns:
            trigram_index_sql = f"""
            CREATE INDEX IF NOT EXISTS {entity.table_name}_{column}_trigram_idx 
            ON {entity.table_name} USING GIN ({column} gin_trgm_ops);
            """
            await self._execute_sql(db, trigram_index_sql)

        content_columns_sql = " || ' ' || ".join(
            [f"NEW.{col}" for col in entity.content_columns]
        )
        trigger_sql = f"""
        CREATE OR REPLACE FUNCTION update_{entity.table_name}_search_vector() 
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.search_vector := to_tsvector('english', 
                COALESCE({content_columns_sql}, ''));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS {entity.table_name}_search_vector_trigger ON {entity.table_name};
        CREATE TRIGGER {entity.table_name}_search_vector_trigger
            BEFORE INSERT OR UPDATE ON {entity.table_name}
            FOR EACH ROW EXECUTE FUNCTION update_{entity.table_name}_search_vector();
        """
        await self._execute_sql(db, trigger_sql)

    async def _create_search_functions(self, db: Session) -> None:
        """Create helper functions for search"""
        highlight_function = """
        CREATE OR REPLACE FUNCTION search_highlight(
            content TEXT, 
            query TEXT
        ) RETURNS TEXT AS $$
        BEGIN
            RETURN ts_headline('english', content, plainto_tsquery('english', query),
                'StartSel=<mark>, StopSel=</mark>, MaxWords=50, MinWords=15');
        END;
        $$ LANGUAGE plpgsql;
        """
        await self._execute_sql(db, highlight_function)

    async def search(self, db: Optional[Session], search_query: SearchQuery) -> SearchResults:
        """
        Perform full-text search across entities. Safe empty response when DB is absent.
        """
        start_time = datetime.now()

        if db is None:
            took_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResults(
                query=search_query.query,
                total_count=0,
                results=[],
                took_ms=took_ms,
                suggestions=[],
            )

        # Build search query
        query_parts: List[str] = []
        params = {
            "tenant_id": self.tenant_id,
            "search_query": search_query.query,
            "limit": search_query.limit,
            "offset": search_query.offset,
        }

        entities_to_search = search_query.entity_types or list(
            self.searchable_entities.keys()
        )

        for entity_type in entities_to_search:
            if entity_type not in self.searchable_entities:
                continue

            entity = self.searchable_entities[entity_type]

            title_col = (
                entity.title_columns[0]
                if entity.title_columns
                else f"'{entity_type}'"
            )
            description_cols = " || ' ' || ".join(
                [f"{col}" for col in entity.content_columns[:3]]
            ) or "''"
            metadata_cols = ", ".join(
                [f"'{col}', {col}" for col in entity.metadata_columns]
            ) or "'source','pg'"
            tenant_condition = (
                f"{entity.tenant_column} = :tenant_id AND"
                if entity.tenant_column
                else ""
            )

            if search_query.fuzzy:
                similarity_cols = " + ".join(
                    [
                        f"similarity({col}, :search_query)"
                        for col in entity.content_columns[:3]
                    ]
                ) or "0"
                trigram_conditions = " OR ".join(
                    [f"{col} % :search_query" for col in entity.content_columns]
                ) or "FALSE"
                entity_query = f"""
                (
                    SELECT 
                        '{entity_type}' as entity_type,
                        {entity.id_column}::text as entity_id,
                        GREATEST(
                            ts_rank(search_vector, plainto_tsquery('english', :search_query)),
                            {similarity_cols}
                        ) as score,
                        {title_col} as title,
                        COALESCE({description_cols}, '') as description,
                        json_build_object({metadata_cols}) as metadata
                    FROM {entity.table_name}
                    WHERE 
                        {tenant_condition}
                        (
                            search_vector @@ plainto_tsquery('english', :search_query)
                            OR {trigram_conditions}
                        )
                        {self._build_filters_sql(search_query.filters, entity)}
                )
                """
            else:
                entity_query = f"""
                (
                    SELECT 
                        '{entity_type}' as entity_type,
                        {entity.id_column}::text as entity_id,
                        ts_rank(search_vector, plainto_tsquery('english', :search_query)) as score,
                        {title_col} as title,
                        COALESCE({description_cols}, '') as description,
                        json_build_object({metadata_cols}) as metadata
                    FROM {entity.table_name}
                    WHERE 
                        {tenant_condition}
                        search_vector @@ plainto_tsquery('english', :search_query)
                        {self._build_filters_sql(search_query.filters, entity)}
                )
                """

            query_parts.append(entity_query)

        if not query_parts:
            took_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResults(
                query=search_query.query, total_count=0, results=[], took_ms=took_ms
            )

        full_query = f"""
        WITH search_results AS (
            {" UNION ALL ".join(query_parts)}
        )
        SELECT * FROM search_results 
        WHERE score > 0.1
        ORDER BY score DESC, entity_type
        LIMIT :limit OFFSET :offset
        """

        try:
            result = await self._execute_sql(db, full_query, params)
            rows = result.fetchall()
            search_results: List[SearchResult] = []
            for row in rows:
                # Row may be RowMapping or tuple-like depending on driver
                entity_type = getattr(row, "entity_type", row[0])
                entity_id = str(getattr(row, "entity_id", row[1]))
                score = float(getattr(row, "score", row[2]))
                title = getattr(row, "title", row[3])
                description = getattr(row, "description", row[4])
                metadata = getattr(row, "metadata", row[5])
                if not isinstance(metadata, dict):
                    metadata = {}

                search_results.append(
                    SearchResult(
                        entity_type=entity_type,
                        entity_id=entity_id,
                        score=score,
                        title=title,
                        description=description,
                        metadata=metadata,
                    )
                )

            count_query = f"""
            WITH search_results AS (
                {" UNION ALL ".join(query_parts)}
            )
            SELECT COUNT(*) as total FROM search_results WHERE score > 0.1
            """
            count_result = await self._execute_sql(db, count_query, params)
            total_row = count_result.fetchone()
            total_count = int(getattr(total_row, "total", total_row[0])) if total_row else 0

            suggestions: List[str] = []
            if not search_results:
                suggestions = await self._get_suggestions(db, search_query.query)

            took_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResults(
                query=search_query.query,
                total_count=total_count,
                results=search_results,
                took_ms=took_ms,
                suggestions=suggestions,
            )
        except Exception as e:
            logger.warning(f"search failed (returning empty): {e}")
            took_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            return SearchResults(
                query=search_query.query, total_count=0, results=[], took_ms=took_ms
            )

    async def index_entity(
        self, db: Optional[Session], entity_type: str, entity_id: str, data: Dict[str, Any]
    ) -> bool:
        """Index or reindex an entity. Safe no-op without DB."""
        if db is None:
            return True
        try:
            entity = self.searchable_entities.get(entity_type)
            if not entity:
                return False
            sql = f"UPDATE {entity.table_name} SET updated_at = NOW() WHERE {entity.id_column} = :id"
            await self._execute_sql(db, sql, {"id": entity_id})
            return True
        except Exception as e:
            logger.warning(f"index_entity failed: {e}")
            return False

    async def _get_suggestions(self, db: Session, query: str) -> List[str]:
        """Very simple suggestions implementation using trigram similarity on common columns."""
        try:
            suggestions: List[str] = []
            for entity in self.searchable_entities.values():
                if not entity.content_columns:
                    continue
                col = entity.content_columns[0]
                sql = (
                    f"SELECT DISTINCT {col} FROM {entity.table_name} "
                    f"WHERE {col} % :q ORDER BY similarity({col}, :q) DESC LIMIT 3"
                )
                res = await self._execute_sql(db, sql, {"q": query})
                for row in res.fetchall():
                    value = row[0]
                    if isinstance(value, str):
                        suggestions.append(value)
                if len(suggestions) >= 5:
                    break
            return suggestions[:5]
        except Exception as e:
            logger.debug(f"suggestions failed: {e}")
            return []

    async def _execute_sql(self, db: Session, sql: str, params: Optional[Dict[str, Any]] = None):
        """Helper to execute SQL and return a DBAPI-like result."""
        return db.execute(text(sql), params or {})

    def _build_filters_sql(self, filters: Dict[str, Any], entity: SearchableEntity) -> str:
        """Build simple AND filters on metadata/content columns. Sanitized by SQLAlchemy params upstream."""
        if not filters:
            return ""
        clauses: List[str] = []
        for key in filters.keys():
            if key in entity.metadata_columns or key in entity.content_columns:
                clauses.append(f"{key} = :{key}")
        if not clauses:
            return ""
        return " AND (" + " AND ".join(clauses) + ")"
