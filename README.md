<!-- NOTE: All sections are placeholders. Use the relevant ones-->

![Logo](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/th5xamgrr6se0x5ro4g6.png)

<!-- Make a favicon/logo using something like:

* https://favicon.io/
* https://www.shopify.com/tools/logo-maker/open-source-software
* https://primitive.lol/ -->

## Badges

<!-- You can get project relevant badges from: [shields.io](https://shields.io/) -->

[![Update gh-pages Docs](https://github.com/HakaiInstitute/ocean-data-parser/actions/workflows/generate-documentation.yaml/badge.svg)](https://github.com/HakaiInstitute/ocean-data-parser/actions/workflows/generate-documentation.yaml)
[![pages-build-deployment](https://github.com/HakaiInstitute/ocean-data-parser/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/HakaiInstitute/ocean-data-parser/actions/workflows/pages/pages-build-deployment)

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

### Testing
To run the different tests, you can use pytest with the command:

```
pytest
```

## Contributing

Contributions are welcome!

See `contributing.md` to get started.