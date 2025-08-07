import functools
import inspect
from typing import Callable

from prefect import task, get_run_logger
from prefect.blocks.core import Block
from prefect.runtime import task_run

def observable_task(func: Callable) -> Callable:
    """
    A decorator that turns a function into a Prefect task with enhanced observability.

    This decorator automatically logs the following to the Prefect UI:
    - Task inputs (arguments)
    - Task outputs (return value)
    - Exceptions raised during the task run
    - Any Prefect Blocks used within the task (from arguments or global scope)
    """
    # Register the decorated function as a Prefect task
    # The task name is automatically inferred from the function name.
    @task
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_run_logger()
        task_name = func.__name__

        # Get parameters from the Prefect runtime context for robust logging
        parameters = task_run.parameters or {}
        logger.info(f"Task '{task_name}' called with parameters: {parameters}")

        try:
            # --- Block Detection Logic ---
            # Inspect the function's arguments and globals to find any Block instances
            used_blocks = []
            detected_slugs = set() # To avoid logging the same block twice

            # 1. Check passed arguments (from runtime parameters)
            all_arg_values = list(parameters.values())
            for obj in all_arg_values:
                if isinstance(obj, Block):
                    slug = obj.get_block_slug() if hasattr(obj, 'get_block_slug') else 'N/A'
                    if slug not in detected_slugs:
                        block_info = {
                            "source": "Passed as Argument",
                            "block_type": type(obj).__name__,
                            "block_slug": slug
                        }
                        used_blocks.append(block_info)
                        detected_slugs.add(slug)

            # 2. Check globals of the function's module for any other blocks
            for name, obj in inspect.getmembers(func.__globals__):
                if isinstance(obj, Block):
                    slug = obj.get_block_slug() if hasattr(obj, 'get_block_slug') else 'N/A'
                    if slug not in detected_slugs:
                        block_info = {
                            "source": "Global Scope",
                            "variable_name": name,
                            "block_type": type(obj).__name__,
                            "block_slug": slug
                        }
                        used_blocks.append(block_info)
                        detected_slugs.add(slug)
            
            if used_blocks:
                 logger.info(f"Task '{task_name}' is using the following Prefect Blocks: {used_blocks}")
            else:
                logger.info(f"Task '{task_name}' did not detect any Prefect Blocks.")

            # Execute the original function. Prefect handles passing the correct args/kwargs here.
            result = func(*args, **kwargs)

            # Log task output
            logger.info(f"Task '{task_name}' completed successfully with output: {result}")
            
            return result

        except Exception as e:
            # Log any exceptions
            logger.error(f"Task '{task_name}' failed with exception: {e}", exc_info=True)
            raise

    return wrapper
