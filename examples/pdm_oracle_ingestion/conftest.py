def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: end-to-end test that requires the docker-compose stack to be up",
    )
