from prefect import flow, task


@task
def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@flow(name="example-flow")
def example_flow(name: str = "world") -> None:
    message = say_hello(name)
    print(message)


if __name__ == "__main__":
    example_flow()
