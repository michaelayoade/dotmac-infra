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
    entity_types: List[str] = Field(default_factory=list, description="Entity types to search")
    filters: Dict[str, Any] = Field(default_factory=dict, description="Additional filters")
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

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.searchable_entities = self._get_searchable_entities()
        
    def _get_searchable_entities(self) -> Dict[str, SearchableEntity]:
        """Configure searchable entities"""
        return {
            "contact": SearchableEntity(
                entity_type="contact",
                table_name="contacts",
                title_columns=["first_name", "last_name", "company"],
                content_columns=["first_name", "last_name", "middle_name", "title", "company", "department"],
                metadata_columns=["contact_type", "created_at", "updated_at"]
            ),
            "address": SearchableEntity(
                entity_type="address", 
                table_name="addresses",
                title_columns=["street_address", "city"],
                content_columns=["street_address", "street_address_2", "city", "state_province", "postal_code", "country"],
                metadata_columns=["address_type", "created_at"]
            ),
            "email": SearchableEntity(
                entity_type="email",
                table_name="emails", 
                title_columns=["email_address"],
                content_columns=["email_address"],
                metadata_columns=["email_type", "is_verified", "is_primary"]
            ),
            "phone": SearchableEntity(
                entity_type="phone",
                table_name="phones",
                title_columns=["phone_number"],
                content_columns=["phone_number", "extension"],
                metadata_columns=["phone_type", "is_verified", "is_primary"]
            ),
            "organization": SearchableEntity(
                entity_type="organization",
                table_name="organizations",
                title_columns=["name"],
                content_columns=["name", "description", "industry", "tax_id"],
                metadata_columns=["organization_type", "created_at"]
            ),
            "customer": SearchableEntity(
                entity_type="customer",
                table_name="customers",
                title_columns=["portal_id"],
                content_columns=["portal_id"],
                metadata_columns=["customer_type", "status", "created_at"]
            )
        }

    async def setup_search_infrastructure(self, db: Session) -> bool:
        """
        Setup PostgreSQL search infrastructure
        Creates extensions, indexes, and search functions
        """
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

    async def _create_search_index(self, db: Session, entity: SearchableEntity):
        """Create FTS and trigram indexes for entity"""
        
        # Create tsvector column for full-text search
        tsvector_sql = f"""
        ALTER TABLE {entity.table_name} 
        ADD COLUMN IF NOT EXISTS search_vector tsvector;
        """
        await self._execute_sql(db, tsvector_sql)
        
        # Create GIN index on tsvector
        gin_index_sql = f"""
        CREATE INDEX IF NOT EXISTS {entity.table_name}_search_vector_idx 
        ON {entity.table_name} USING GIN (search_vector);
        """
        await self._execute_sql(db, gin_index_sql)
        
        # Create trigram indexes for fuzzy search
        for column in entity.content_columns:
            trigram_index_sql = f"""
            CREATE INDEX IF NOT EXISTS {entity.table_name}_{column}_trigram_idx 
            ON {entity.table_name} USING GIN ({column} gin_trgm_ops);
            """
            await self._execute_sql(db, trigram_index_sql)
        
        # Create trigger to update search_vector automatically
        content_columns_sql = ' || \' \' || '.join([f'NEW.{col}' for col in entity.content_columns])
        trigger_sql = f"""
        CREATE OR REPLACE FUNCTION update_{entity.table_name}_search_vector() 
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.search_vector := to_tsvector('english', 
                COALESCE({content_columns_sql}, '')
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS {entity.table_name}_search_vector_trigger ON {entity.table_name};
        CREATE TRIGGER {entity.table_name}_search_vector_trigger
            BEFORE INSERT OR UPDATE ON {entity.table_name}
            FOR EACH ROW EXECUTE FUNCTION update_{entity.table_name}_search_vector();
        """
        await self._execute_sql(db, trigger_sql)

    async def _create_search_functions(self, db: Session):
        """Create helper functions for search"""
        
        # Function for highlighted search results
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

    async def search(self, db: Session, search_query: SearchQuery) -> SearchResults:
        """
        Perform full-text search across entities
        """
        start_time = datetime.now()
        
        # Build search query
        query_parts = []
        params = {
            'tenant_id': self.tenant_id,
            'search_query': search_query.query,
            'limit': search_query.limit,
            'offset': search_query.offset
        }
        
        # Determine which entities to search
        entities_to_search = search_query.entity_types or list(self.searchable_entities.keys())
        
        for entity_type in entities_to_search:
            if entity_type not in self.searchable_entities:
                continue
                
            entity = self.searchable_entities[entity_type]
            
            # Build entity-specific query
            if search_query.fuzzy:
                # Combine FTS and trigram search
                similarity_cols = ' + '.join([f'similarity({col}, :search_query)' for col in entity.content_columns[:3]])
                title_col = entity.title_columns[0] if entity.title_columns else f"'{entity_type}'"
                description_cols = ' || \' \' || '.join([f'{col}' for col in entity.content_columns[:3]])
                metadata_cols = ', '.join([f"'{col}', {col}" for col in entity.metadata_columns])
                tenant_condition = f"{entity.tenant_column} = :tenant_id AND" if entity.tenant_column else ""
                trigram_conditions = ' OR '.join([f'{col} % :search_query' for col in entity.content_columns])
                
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
                        json_build_object(
                            {metadata_cols}
                        ) as metadata
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
                # FTS only
                title_col = entity.title_columns[0] if entity.title_columns else f"'{entity_type}'"
                description_cols = ' || \' \' || '.join([f'{col}' for col in entity.content_columns[:3]])
                metadata_cols = ', '.join([f"'{col}', {col}" for col in entity.metadata_columns])
                tenant_condition = f"{entity.tenant_column} = :tenant_id AND" if entity.tenant_column else ""
                
                entity_query = f"""
                (
                    SELECT 
                        '{entity_type}' as entity_type,
                        {entity.id_column}::text as entity_id,
                        ts_rank(search_vector, plainto_tsquery('english', :search_query)) as score,
                        {title_col} as title,
                        COALESCE({description_cols}, '') as description,
                        json_build_object(
                            {metadata_cols}
                        ) as metadata
                    FROM {entity.table_name}
                    WHERE 
                        {tenant_condition}
                        search_vector @@ plainto_tsquery('english', :search_query)
                        {self._build_filters_sql(search_query.filters, entity)}
                )
                """
            
            query_parts.append(entity_query)
        
        # Combine all entity queries
        if not query_parts:
            return SearchResults(
                query=search_query.query,
                total_count=0,
                results=[],
                took_ms=0
            )
        
        full_query = f"""
        WITH search_results AS (
            {' UNION ALL '.join(query_parts)}
        )
        SELECT * FROM search_results 
        WHERE score > 0.1
        ORDER BY score DESC, entity_type
        LIMIT :limit OFFSET :offset
        """
        
        # Execute search
        result = await self._execute_sql(db, full_query, params)
        
        # Convert to SearchResult objects
        search_results = []
        for row in result.fetchall():
            search_results.append(SearchResult(
                entity_type=row.entity_type,
                entity_id=row.entity_id,
                score=float(row.score),
                title=row.title,
                description=row.description,
                metadata=row.metadata if isinstance(row.metadata, dict) else {}
            ))
        
        # Get total count
        count_query = f"""
        WITH search_results AS (
            {' UNION ALL '.join(query_parts)}
        )
        SELECT COUNT(*) as total FROM search_results WHERE score > 0.1
        """
        count_result = await self._execute_sql(db, count_query, params)
        total_count = count_result.fetchone().total
        
        # Generate suggestions if no results
        suggestions = []
        if not search_results:
            suggestions = await self._get_suggestions(db, search_query.query)
        
        end_time = datetime.now()
        took_ms = int((end_time - start_time).total_seconds() * 1000)
        
        return SearchResults(
            query=search_query.query,
            total_count=total_count,
            results=search_results,
            took_ms=took_ms,
            suggestions=suggestions
        )

    async def suggest(self, db: Session, partial_query: str, entity_type: str, limit: int = 10) -> List[str]:
        """Get auto-complete suggestions"""
        if entity_type not in self.searchable_entities:
            return []
        
        entity = self.searchable_entities[entity_type]
        
        # Use trigram similarity for suggestions
        suggestions_query = f"""
        SELECT DISTINCT {entity.title_columns[0]} as suggestion
        FROM {entity.table_name}
        WHERE 
            {f"{entity.tenant_column} = :tenant_id AND" if entity.tenant_column else ""}
            {entity.title_columns[0]} % :partial_query
        ORDER BY similarity({entity.title_columns[0]}, :partial_query) DESC
        LIMIT :limit
        """
        
        result = await self._execute_sql(db, suggestions_query, {
            'tenant_id': self.tenant_id,
            'partial_query': partial_query,
            'limit': limit
        })
        
        return [row.suggestion for row in result.fetchall()]

    async def index_entity(self, db: Session, entity_type: str, entity_id: str, 
                          searchable_data: Dict[str, Any]) -> bool:
        """
        Index or re-index a specific entity
        The search_vector will be updated automatically by triggers
        """
        if entity_type not in self.searchable_entities:
            logger.warning(f"Entity type {entity_type} not configured for search")
            return False
        
        entity = self.searchable_entities[entity_type]
        
        # Update the entity record - trigger will update search_vector
        update_fields = []
        params = {'entity_id': entity_id}
        
        for field, value in searchable_data.items():
            if field in entity.content_columns + entity.metadata_columns:
                update_fields.append(f"{field} = :{field}")
                params[field] = value
        
        if not update_fields:
            return True
        
        update_query = f"""
        UPDATE {entity.table_name}
        SET {', '.join(update_fields)}, updated_at = NOW()
        WHERE {entity.id_column} = :entity_id
        """
        
        try:
            await self._execute_sql(db, update_query, params)
            return True
        except Exception as e:
            logger.error(f"Failed to index entity {entity_type}:{entity_id}: {str(e)}")
            return False

    async def _get_suggestions(self, db: Session, query: str, limit: int = 5) -> List[str]:
        """Get search suggestions when no results found"""
        # Simple implementation - can be enhanced
        suggestions_query = """
        SELECT word, similarity(word, :query) as sim
        FROM (
            SELECT DISTINCT unnest(string_to_array(first_name || ' ' || last_name, ' ')) as word
            FROM contacts
            WHERE tenant_id = :tenant_id
            UNION
            SELECT DISTINCT unnest(string_to_array(name, ' ')) as word  
            FROM organizations
            WHERE tenant_id = :tenant_id
        ) words
        WHERE word % :query AND length(word) > 2
        ORDER BY sim DESC
        LIMIT :limit
        """
        
        result = await self._execute_sql(db, suggestions_query, {
            'tenant_id': self.tenant_id,
            'query': query,
            'limit': limit
        })
        
        return [row.word for row in result.fetchall()]

    def _build_filters_sql(self, filters: Dict[str, Any], entity: SearchableEntity) -> str:
        """Build additional filter conditions"""
        if not filters:
            return ""
        
        conditions = []
        for field, value in filters.items():
            if field in entity.metadata_columns + entity.content_columns:
                if isinstance(value, list):
                    conditions.append(f"{field} = ANY(ARRAY{value})")
                else:
                    conditions.append(f"{field} = '{value}'")
        
        return f" AND {' AND '.join(conditions)}" if conditions else ""

    async def _execute_sql(self, db: Session, sql: str, params: Dict = None):
        """Execute SQL with error handling"""
        try:
            if params:
                return db.execute(text(sql), params)
            else:
                return db.execute(text(sql))
        except Exception as e:
            logger.error(f"SQL execution failed: {sql[:100]}... Error: {str(e)}")
            raise

    async def get_search_stats(self, db: Session) -> Dict[str, Any]:
        """Get search infrastructure statistics"""
        stats = {}
        
        for entity_type, entity in self.searchable_entities.items():
            # Get index sizes and row counts
            stats_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN search_vector IS NOT NULL THEN 1 END) as indexed_rows
            FROM {entity.table_name}
            WHERE {f"{entity.tenant_column} = :tenant_id" if entity.tenant_column else "1=1"}
            """
            
            result = await self._execute_sql(db, stats_query, {'tenant_id': self.tenant_id})
            row = result.fetchone()
            
            stats[entity_type] = {
                'total_rows': row.total_rows,
                'indexed_rows': row.indexed_rows,
                'index_coverage': (row.indexed_rows / row.total_rows * 100) if row.total_rows > 0 else 0
            }
        
        return stats
