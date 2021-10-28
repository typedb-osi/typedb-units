# Simplest TypeQL Units Model
authors: @modeller and @bradeneliason

date: September 2021

## Overview
This repo is the simplest TypeQL model of a units library. It contains the code and data needed to load a minimum example. It imports data froma series of csv, making it easy to add your own units.

## How it's used
Units are defined by their string value, for example "kN.m", or "m2". This "unit" attribute, is also owned as a key by the ```unit_details``` relation, which contains all of the details for that particular unit. These details include conversion attributes, to enable conversion to the metric base unit. 

To create a measure, one needs to simply have a quantity and connect the unit to it in the schema
```measure isa attribute, value long, has unit```

Then, on import one just uses the simple syntax
```insert $m isa measure; $m 103.25; $m has unit "kPa";```

## How its built
Units are of different types:
- base units are units with a single dimension. They include SI base units plus any other reference value used as dimension (radians, currency, bits, threads etc.) 
- core units are combinations of base units with multiple dimensions (or inverse dimension like 1/Time) without any scaling factors.
- scaled units are ones created out of base or core through scaling, and they link back to their core unit for conversion purposes (implemented as a rule)
- imperial units are just a form of scaling

The unit is also connected to the dimensions, which define that measure. These dimensions also have the base unit name, attached to them. In addition to the usual SI dimensions (time, length, mass, current, temperature, moles, luminosity), radians is a provided as a base unit for plane angles. Other non-dimensional quantities are provided as a dimension type with zero values for all dimension components. 

Non-base units, like "in2", are automatically connected to their base unit, "m2", so that conversions can be set up. At the moment, conversions are not implemented, as we are waiting for calculations before implementing them.

## Tolerances

For this schema all tolerance types are converted into bilateral form with an upper and a lower tolernace value. How the upper and lower tolerance values are interpreted depends on two boolean values `is_absolute` and `is_offset`:
 - If `is_absolute` is true the upper and lower tolerance value have the same units as the nominal value, otherwise the upper and lower tolerances are interpreted as a scaling factor of the nominal value. 
 - If `is_offset` is true, the limits of the tolernace is computed by adding the upper and lower tolernace to the nominal value, otherwise, the upper and lower tolerance stand alone as the limits. 

The table below shows how the tolerance `10±1` can be represented in four ways. 

|                         | **is_absolute<br>true** | **is_absolute<br>false** |
|-------------------------|:-----------------------:|:------------------------:|
| **is_offset<br>true**   | +1 <br> -1              | (×) +0.1 <br> -0.1       |
| **is_offset<br>false**  | 11 <br> 9               | (×) 1.10 <br> 0.90       |

## Installing the Repo

### Prerequisites
To install the repo make sure that pipenv is installed in your Python. 

The implementation is tested on Python 3.8.

TypeDB must be running, and the system is setup to use the localhost. If you are using a different server, then its address need to be inserted in the clean_and_load.py.

### Installation
1. Run Pipenv shell
```pipenv shell```
2. Run pipenv install
```pipenv install```
3. Run clean_and_load.py
```python clean_and_load.py```

The installation implements some example measures, and these can be easily extended by adjusting the csv files from the spreadsheet
