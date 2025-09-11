# Troubleshooting

## Docker PermissionError in tests

When running tests that use `testcontainers` (like `tests/test_loader_postgres.py`), you might encounter a `docker.errors.DockerException` with a `PermissionError: [Errno 13] Permission denied`.

This error occurs because the user running the tests does not have permission to access the Docker daemon socket at `/var/run/docker.sock`.

### Solution

In a sandboxed or development environment, you can resolve this by changing the permissions of the Docker socket:

```bash
sudo chmod 666 /var/run/docker.sock
```

This will make the socket world-writable, allowing the tests to connect to the Docker daemon.

**Note:** This is not recommended for production environments due to security implications. In a production-like setup, the user should be added to the `docker` group, and the shell session should be restarted.
