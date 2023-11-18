Contributing to Pynenc
======================

Thank you for your interest in contributing to Pynenc! At this moment, the project is in its initial development phase and is not yet open for external contributions. We are diligently working towards a Minimum Viable Product (MVP) and establishing a stable foundation.

Once the project reaches a stage where community contributions can be integrated, this guide will be updated with detailed instructions on the contribution process. For now, we invite you to watch the repository for updates and participate in issue discussions.

We appreciate your understanding and look forward to your contributions in the future!

Best regards,
The Pynenc Team

Setting Up the Development Environment
---------------------------------------

To contribute to Pynenc once it's open for contributions, follow these typical steps to set up your development environment:

1. **Fork the Repository**: Start by forking the Pynenc repository (https://github.com/pynenc/pynenc) on GitHub to your own account.

2. **Clone the Fork**: Clone your fork to your local machine.
   
   .. code-block:: bash

       git clone https://github.com/your-username/pynenc.git
       cd pynenc

3. **Install Docker**: Make sure you have Docker installed on your system as it may be used for running services such as databases or other dependencies.

   - Docker Installation: Visit https://docs.docker.com/get-docker/

4. **Install Poetry**: Pynenc uses Poetry for dependency management. Install Poetry using the recommended method from the official documentation at https://python-poetry.org/docs/#installation.

5. **Set Up the Project**: Inside the project directory, set up your local development environment using Poetry.

   .. code-block:: bash

       poetry install

6. **Activate the Virtual Environment**: Use Poetry to activate the virtual environment.

   .. code-block:: bash

       poetry shell

7. **Start Development**: You are now ready to start development. Make changes, commit them, and push them to your fork.

8. **Creating Pull Requests**: Once the project is open for contributions, you will be able to create pull requests from your fork to the main Pynenc repository.

Remember, these steps will become applicable once the project is open for contributions. Until then, feel free to familiarize yourself with the codebase and the project's goals.
