# HCRA Ontario API

## Introduction

The `hcraontario-API` Python package is designed to facilitate efficient access to the HCRA (Home Construction Regulatory Authority) Ontario database. It enables users to perform detailed queries on registered builders and retrieve comprehensive information, including summary, projects, convictions, and more. This tool is invaluable for research and data analysis in the housing and construction sectors, offering an easy-to-use interface for extracting detailed builder records.

## Features

- **Advanced Search Functionality:** Execute detailed searches using a variety of filters such as builder name, location, and licence status.
- **Comprehensive Information Retrieval:** Access in-depth details about builders, including their projects, legal convictions, conditions, properties, and more.
- **Concurrent Data Fetching:** Utilizes Python's `concurrent.futures` module for efficient data retrieval, reducing wait times by fetching data in parallel.

## Installation

To install `hcraontario-API`, you need to have Python installed on your machine. Then, run the following command in your terminal:

```bash
pip install hcraontario-api
```

(Note: This package is hypothetical and the installation command is for demonstration purposes.)

## Usage

### Importing the Module

To get started, import the `Hcraontario` class from the package:

```python
from hcraontario import Hcraontario
```

### Creating an Instance

Instantiate the `Hcraontario` class:

```python
api = Hcraontario.API()
```

### Search Builders

Conduct searches for builders based on specific criteria.

#### Search Parameters

| Parameter       | Type | Description                                 |
| --------------- | ---- | ------------------------------------------- |
| builderName     | str  | Name of the builder (optional).             |
| builderLocation | str  | Location of the builder (optional).         |
| builderNum      | str  | Number of the builder (optional).           |
| officerDirector | str  | Name of the officer or director (optional). |
| umbrellaCo      | str  | Umbrella company (optional).                |
| licenceStatus   | str  | License status (optional).                  |
| yearsActive     | str  | Years of activity (optional).               |

**Returns:**

- A list of dictionaries, each representing a builder that matches the search criteria.

```python
search_results = api.search_builder(builderLocation="Toronto")
print(search_results)
```

### Fetching Builder Details

Retrieve comprehensive information about a specific builder using their unique ID.

```python
builder_info = api.get_builder_detail(ID="B60767")
print(builder_info)
```

**Returns:**

- A dictionary with detailed information about the builder, including:
  * `summary`: Builder's summary.
  * `PDOs`: Development projects.
  * `convictions`: Legal convictions.
  * `conditions`: Conditions.
  * `members`: Members.
  * `properties`: Properties.
  * `enrolments`: Enrolments.
  * `condoProjects`: Condo projects.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.
