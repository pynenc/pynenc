# README for Pynenc Trigger System

This project implements a trigger system for task scheduling and event-driven execution. The system is designed to evaluate various conditions under which tasks should be triggered, providing a flexible and extensible framework for managing task execution based on specific criteria.

## Project Structure

The project is organized into the following main components:

- **trigger**: Contains the core logic for the trigger system.
  - **conditions**: A subdirectory that houses various condition classes used to determine when tasks should be triggered.
    - **base.py**: Defines the `TriggerCondition` class, the base class for all trigger conditions.
    - **time.py**: Contains the `CronContext` class for time-based conditions.
    - **event.py**: Defines the `EventContext` class for event-based conditions.
    - **status.py**: Contains the `StatusContext` class for task status conditions.
    - **result.py**: Defines the `ResultContext` class for result-based conditions.
    - **composite.py**: Contains the `CompositeCondition` class for combining multiple conditions using logical operators.
  - **trigger/condition/base.py**: Contains the `TriggerCondition` and `ConditionContext` classes and its subclasses, providing the structures for condition evaluation.

## Usage

To use the Pynenc trigger system, you can create instances of the various condition classes and combine them as needed to define your task triggering logic. The system supports both simple and complex conditions, allowing for flexible task management.

## Contributing

Contributions to the Pynenc trigger system are welcome. Please follow the standard practices for contributing to open-source projects, including forking the repository, making your changes, and submitting a pull request.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
