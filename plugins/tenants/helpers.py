"""
Tenant management helpers for multi-tenant Airflow.

These utilities help enforce tenant isolation and provide tenant-specific
configuration access.
"""

import re
from typing import Optional, List, Dict, Any
from airflow.models import Variable, Connection
from airflow.hooks.base import BaseHook


# Known tenants registry (can be extended)
KNOWN_TENANTS = {
    'data-engineering',
    'analytics',
    'ml-team',
    'data-science',
    'platform',
    'devops',
}


def get_tenant_from_dag_id(dag_id: str) -> Optional[str]:
    """
    Extract tenant from DAG ID.
    
    DAG IDs should follow format: {tenant}_{dag_name}
    
    Args:
        dag_id: The DAG ID
        
    Returns:
        The tenant identifier, or None if format is invalid
        
    Example:
        >>> get_tenant_from_dag_id('data-engineering_daily_etl')
        'data-engineering'
    """
    if not dag_id or '_' not in dag_id:
        return None
    
    parts = dag_id.split('_', 1)
    if len(parts) != 2:
        return None
    
    tenant = parts[0]
    
    # Validate tenant format
    if not re.match(r'^[a-z0-9-]+$', tenant):
        return None
    
    return tenant


def validate_tenant(tenant: str) -> bool:
    """
    Validate that a tenant identifier is valid.
    
    Args:
        tenant: The tenant identifier
        
    Returns:
        True if valid, False otherwise
    """
    if not tenant:
        return False
    
    # Check format
    if not re.match(r'^[a-z0-9-]+$', tenant):
        return False
    
    # Check if in known tenants (warning only, not a hard requirement)
    # This allows new tenants to be added dynamically
    
    return True


def get_tenant_connections(tenant: str, conn_prefix: Optional[str] = None) -> List[Connection]:
    """
    Get all connections for a tenant.
    
    Connections should follow naming: {tenant}_{connection_name}
    
    Note: This is a placeholder. Full implementation would require
    querying Airflow's connection metadata, which is best done via
    Airflow's internal APIs or database queries.
    
    Args:
        tenant: The tenant identifier
        conn_prefix: Optional connection name prefix to filter
        
    Returns:
        List of Connection objects matching the tenant prefix
    """
    # Placeholder - would need Airflow metadata access
    return []


def get_tenant_variables(tenant: str, var_prefix: Optional[str] = None) -> Dict[str, Any]:
    """
    Get all variables for a tenant.
    
    Variables should follow naming: {tenant}_{variable_name}
    
    Note: This is a placeholder. Full implementation would require
    querying Airflow's variable metadata, which is best done via
    Airflow's internal APIs or database queries.
    
    Args:
        tenant: The tenant identifier
        var_prefix: Optional variable name prefix to filter
        
    Returns:
        Dictionary of variable name -> value for tenant variables
    """
    # Placeholder - would need Airflow metadata access
    return {}


def get_connection_for_tenant(tenant: str, connection_name: str) -> Optional[Connection]:
    """
    Get a specific connection for a tenant.
    
    Connection ID format: {tenant}_{connection_name}
    
    Args:
        tenant: The tenant identifier
        connection_name: The connection name (without tenant prefix)
        
    Returns:
        Connection object, or None if not found
    """
    conn_id = f"{tenant}_{connection_name}"
    
    try:
        return BaseHook.get_connection(conn_id=conn_id)
    except Exception:
        return None


def get_variable_for_tenant(tenant: str, variable_name: str, default: Any = None) -> Any:
    """
    Get a specific variable for a tenant.
    
    Variable key format: {tenant}_{variable_name}
    
    Args:
        tenant: The tenant identifier
        variable_name: The variable name (without tenant prefix)
        default: Default value if variable not found
        
    Returns:
        Variable value, or default if not found
    """
    var_key = f"{tenant}_{variable_name}"
    
    try:
        return Variable.get(var_key, default_var=default)
    except Exception:
        return default


def ensure_tenant_isolation(dag_id: str, tenant: Optional[str] = None) -> str:
    """
    Ensure DAG ID follows tenant isolation pattern.
    
    If tenant is provided, validates the DAG ID matches.
    If tenant is None, extracts tenant from DAG ID.
    
    Args:
        dag_id: The DAG ID
        tenant: Optional tenant to validate against
        
    Returns:
        The tenant identifier
        
    Raises:
        ValueError: If DAG ID doesn't follow tenant pattern or doesn't match provided tenant
    """
    extracted_tenant = get_tenant_from_dag_id(dag_id)
    
    if not extracted_tenant:
        raise ValueError(
            f"DAG ID '{dag_id}' must follow format: {{tenant}}_{{name}}"
        )
    
    if tenant and extracted_tenant != tenant:
        raise ValueError(
            f"DAG ID '{dag_id}' has tenant '{extracted_tenant}' but expected '{tenant}'"
        )
    
    if not validate_tenant(extracted_tenant):
        raise ValueError(
            f"Invalid tenant format: '{extracted_tenant}' (must be lowercase, alphanumeric, hyphens)"
        )
    
    return extracted_tenant

