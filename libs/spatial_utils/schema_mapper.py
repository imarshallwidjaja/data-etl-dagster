# =============================================================================
# Schema Name Mapper - Run ID to PostgreSQL Schema Mapping
# =============================================================================
# Provides bidirectional conversion between Dagster run_id and PostgreSQL
# schema names for ephemeral processing schemas.
# =============================================================================

from pydantic import BaseModel, Field, field_validator

__all__ = ["RunIdSchemaMapping"]


class RunIdSchemaMapping(BaseModel):
    """
    Bidirectional mapping between Dagster run_id and PostgreSQL schema_name.
    
    Enables safe, reversible conversion of run IDs to valid PostgreSQL identifiers.
    
    Attributes:
        run_id: Original Dagster run ID (UUID format)
        schema_name: PostgreSQL schema name (safe identifier)
    
    Example:
        >>> mapping = RunIdSchemaMapping.from_run_id("abc12345-def6-7890-ijkl-mnop12345678")
        >>> mapping.schema_name
        'proc_abc12345_def6_7890_ijkl_mnop12345678'
        
        >>> # Reverse mapping
        >>> reverse = RunIdSchemaMapping.from_schema_name(mapping.schema_name)
        >>> reverse.run_id
        'abc12345-def6-7890-ijkl-mnop12345678'
    """
    
    run_id: str = Field(..., description="Original Dagster run ID (UUID format)")
    schema_name: str = Field(..., description="PostgreSQL schema name (safe identifier)")
    
    @field_validator("run_id")
    @classmethod
    def validate_run_id(cls, v: str) -> str:
        """
        Validate that run_id is a valid UUID format.
        
        Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        
        Args:
            v: Run ID string to validate
            
        Returns:
            Validated run_id
            
        Raises:
            ValueError: If format is invalid
        """
        if not v:
            raise ValueError("run_id cannot be empty")
        
        # Check basic UUID format: 8-4-4-4-12 hex chars with hyphens
        parts = v.split("-")
        if len(parts) != 5:
            raise ValueError(
                f"run_id must be UUID format (8-4-4-4-12), got: {v}"
            )
        
        expected_lengths = [8, 4, 4, 4, 12]
        for part, expected_len in zip(parts, expected_lengths):
            if len(part) != expected_len:
                raise ValueError(
                    f"run_id UUID part has wrong length: {part} (expected {expected_len})"
                )
            # Check all chars are hex
            try:
                int(part, 16)
            except ValueError:
                raise ValueError(
                    f"run_id UUID part contains non-hex characters: {part}"
                )
        
        return v
    
    @field_validator("schema_name")
    @classmethod
    def validate_schema_name(cls, v: str) -> str:
        """
        Validate that schema_name is a valid PostgreSQL identifier.
        
        Args:
            v: Schema name to validate
            
        Returns:
            Validated schema_name
            
        Raises:
            ValueError: If format is invalid
        """
        if not v:
            raise ValueError("schema_name cannot be empty")
        
        if not v.startswith("proc_"):
            raise ValueError(
                f"schema_name must start with 'proc_', got: {v}"
            )
        
        # PostgreSQL identifiers: lowercase letters, numbers, underscores
        # Must start with letter or underscore
        for char in v:
            if not (char.isalnum() or char == "_"):
                raise ValueError(
                    f"schema_name contains invalid character '{char}': {v}"
                )
        
        return v
    
    @classmethod
    def from_run_id(cls, run_id: str) -> "RunIdSchemaMapping":
        """
        Create mapping by sanitizing a Dagster run_id.
        
        Converts UUID format by replacing hyphens with underscores:
        xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx → proc_xxxxxxxx_xxxx_xxxx_xxxx_xxxxxxxxxxxx
        
        Args:
            run_id: Dagster run ID (UUID format)
            
        Returns:
            RunIdSchemaMapping instance
            
        Raises:
            ValueError: If run_id format is invalid
        """
        schema_name = f"proc_{run_id.replace('-', '_')}"
        return cls(run_id=run_id, schema_name=schema_name)
    
    @classmethod
    def from_schema_name(cls, schema_name: str) -> "RunIdSchemaMapping":
        """
        Create mapping by extracting run_id from schema_name.
        
        Reverses sanitization by extracting the UUID parts and restoring hyphens
        at standard UUID positions (8, 13, 18, 23 in the original UUID string).
        
        Args:
            schema_name: PostgreSQL schema name (format: proc_xxxxxxxx_xxxx_xxxx_xxxx_xxxxxxxxxxxx)
            
        Returns:
            RunIdSchemaMapping instance
            
        Raises:
            ValueError: If schema_name format is invalid
        """
        # Validate prefix
        if not schema_name.startswith("proc_"):
            raise ValueError(
                f"schema_name must start with 'proc_', got: {schema_name}"
            )
        
        # Extract the sanitized part (remove 'proc_' prefix)
        sanitized = schema_name[5:]  # Remove 'proc_'
        
        # Replace underscores back with hyphens at UUID positions
        # UUID parts: 8 chars, 4 chars, 4 chars, 4 chars, 12 chars
        # In sanitized form with underscores: xxxxxxxx_xxxx_xxxx_xxxx_xxxxxxxxxxxx
        try:
            run_id = (
                f"{sanitized[0:8]}-"      # First 8 chars
                f"{sanitized[9:13]}-"     # Skip underscore, take 4
                f"{sanitized[14:18]}-"    # Skip underscore, take 4
                f"{sanitized[19:23]}-"    # Skip underscore, take 4
                f"{sanitized[24:36]}"     # Skip underscore, take 12
            )
        except IndexError:
            raise ValueError(
                f"schema_name is malformed: {schema_name} "
                "(expected format: proc_xxxxxxxx_xxxx_xxxx_xxxx_xxxxxxxxxxxx)"
            )
        
        return cls(run_id=run_id, schema_name=schema_name)
