
## Principles
- Optimise for maintainability: code is read more often than written.


## Coding Standards
- Type hints are required on all functions.
- Add docstrings to all classes and functions.
- Express intent in naming and avoid abbreviations.
- Use comments (only) for what the code cannot say.


## Preferred Tools
- **Poetry** for dependency management
- **Polars** for data loading and mining (prefer over Pandas whenever possible)
- **Seaborn** for plotting (usually works with Polars data as input)
- **Ruff** for linting
- **tqdm** for progress tracking
- **Optuna** for hyperparameter tuning
- **Pydantic** for object validation
