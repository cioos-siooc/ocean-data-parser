# Ocean Data Parser
## Installation
With miniconda, create a new environement:
```console
conda create --name ocean_parser 
````

Get in the environment:
```console
conda activate ocean_parser
```

Install the present package:
```console
pip install git+https://github.com/HakaiInstitute/ocean-data-parser.git
```


For development, clone locally the package :
```console
git clone git+https://github.com/HakaiInstitute/ocean-data-parser.git
```
and install the package:
```console
cd ocean-data-parser
pip install -e .
```
For development purposes, it is recommended to install the development requirements:
```console
pip install -e ".[dev]"
```

## How to
To parse seabird file:
```python
from ocean_data_parser.read import searbird

PATH_TO_SEABIRD_CNV = "PATH_TO_SEABIRD_CNV"
ds = seabird.cnv(PATH_TO_SEABIRD_CNV)
```

<!-- NOTE: All sections are placeholders. Use the relevant ones-->

![Logo](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/th5xamgrr6se0x5ro4g6.png)

<!-- Make a favicon/logo using something like:

* https://favicon.io/
* https://www.shopify.com/tools/logo-maker/open-source-software
* https://primitive.lol/ -->

## Badges

<!-- You can get project relevant badges from: [shields.io](https://shields.io/) -->

[![Build | Passing](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://img.shields.io/badge/build-passing-brightgreen)    [![Latest Release ](https://img.shields.io/badge/release-v4.16.4-blue.svg)](https://img.shields.io/badge/release-v4.16.4-blue) [![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)

# Ocean Data Parser

The `Ocean Data Parser` is a set of tools capable of parsing oceanographic proprietary data formats to a xarray CIOOS Compliant object. This xarray object can then be use in a number of application.

For more detail, you can review the production documentation here: https://hakaiinstitute.github.io/ocean-data-parser/
and the development documentation at [here](https://hakaiinstitute.github.io/ocean-data-parser/).

---

## Table of Contents

<details>

<summary>Table of Contents</summary>

[Configuration](#configuration)

[Development](#development)

[Tests](#tests)

[Deploying](#deploying)


</details>

---

## Configuration

To run this project, you will need to add the following environment variables to your `.env` file

```env
pip install git+https://github.com/HakaiInstitute/ocean-data-parser.git
```

## Development

Clone the project

```shell
  git clone git+https://github.com/HakaiInstitute/ocean-data-parser.git
```

Go to the project directory

```shell
  cd ocean-data-parser
```

Install dependencies

```shell
  pip install -e ".[dev]"
```

### Documentation
To run a local instance of the documentation webpage, run the command:

```shell
mike serve
```

Any documentation changes to the main and development branches will automatically update respectively the main and dev document pages.

## Test
To run the different tests, you can use pytest with the command:

```
pytest
```
## Deploying

The production branch

`TODO:production_branch_goes_here`

is deployed at: <https://production_url>

---
The testing branch

`TODO:testing_branch_name_goes_here`

is deployed at: <https://testing_url>

<!-- etc... -->
---

To deploy this project run:

```shell
  npm run deploy
```

---

## Contributing

Contributions are welcome!

See `contributing.md` to get started.