from prefect import  flow
from decorator import observable_task
from custom_block import MilvusBlock_2

my_milvus_block = MilvusBlock_2.load("milvus")


@observable_task
def add_numbers(x: int, y: int) -> int:
    """A simple task to demonstrate input/output logging."""
    return x + y

@observable_task
def process_with_milvus(milvus_block: MilvusBlock_2):
    """A task that uses the custom Milvus block passed as an argument."""

    return f"Processed data using Milvus config for host {milvus_block.host}"

@observable_task
def failing_task():
    """A task that always fails to demonstrate exception logging."""
    raise ValueError("This task is designed to fail.")


# --- Flow Definition ---

@flow(name="Observability Demo Flow")
def main_observability_flow():
    """
    A flow that demonstrates the @observable_task decorator with multiple tasks,
    including one that uses a custom block passed as a parameter.
    """
    
    # Run a simple task
    add_numbers(15, 30)
    
    # Run a task that uses our custom block by passing it as an argument
    process_with_milvus(my_milvus_block)

    # Run a task that is expected to fail
    try:
        failing_task()
    except ValueError:
        return "Error occured"


if __name__ == "__main__":
    main_observability_flow()
