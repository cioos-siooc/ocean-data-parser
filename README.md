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
pip install git+git@github.com:JessyBarrette/ocean_data_parser.git
```
## How to
To parse seabird file:
```python
from ocean_data_parser.read import searbird


