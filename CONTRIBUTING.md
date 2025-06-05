# Contributing to HySDS

Thank you for considering contributing to HySDS! This document provides guidelines for contributing to the project.

## Python 3.12 Compatibility

This project now requires Python 3.12 or higher. When contributing code, please ensure:

1. **Use timezone-aware datetimes**:
   - Use `from datetime import datetime, UTC`
   - Use `datetime.now(UTC)` instead of `datetime.utcnow()`
   - Ensure all datetime objects are timezone-aware

2. **Logging**:
   - Use `logger.warning()` instead of the deprecated `logger.warn()`

3. **Dependencies**:
   - All dependencies must be compatible with Python 3.12
   - Update `requirements.in` with any new dependencies
   - Run `pip-compile` to generate updated `requirements.txt`

## Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Create a Pull Request

## Testing

Please ensure all tests pass before submitting a pull request:

```bash
python -m pytest
```

## Code Style

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/)
- Use type hints for all new code
- Include docstrings for all public functions and classes
- Keep lines under 100 characters

## Reporting Issues

When reporting issues, please include:

- Python version
- Operating system
- Steps to reproduce the issue
- Expected vs. actual behavior
- Any relevant error messages or logs

## License

By contributing to HySDS, you agree that your contributions will be licensed under its [LICENSE](LICENSE) file.
