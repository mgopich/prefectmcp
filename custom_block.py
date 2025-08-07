from prefect.blocks.core import Block
from pydantic import SecretStr

# --- Custom Block Definition ---
class MilvusBlock_2(Block):
    """
    A custom block to store Milvus connection details.
    """
    collections: str 
    host: str
    password: SecretStr
    user: str
    port: int

# Register the block type with Prefect so it's recognized
MilvusBlock_2.register_type_and_schema()
