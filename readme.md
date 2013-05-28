# Big Data Behavior v2.0

An easy way to efficiently insert/update large amounts of data using CakePHP.

## Background

CakePHP is very slow at handling large amounts of data, because it inserts it one row at a time, and does several validation queries for each row. This plugin allows you to append each item to a "bundle" which is stored in memory, then save the entire bundle at once using a manual query. It will chunk the bundle if necessary, to split it into a few queries for better performance.

## Requirements

* CakePHP 2.x
* PHP 5.2+
* MySQL

## Installation

1. Add this project to app/Plugin/BigData
2. Load the plugin in app/Config/bootstrap.php:  
```CakePlugin::load('BigData');```
3. Add as a behavior to the model(s):  
```public $actsAs = array('BigData.BigData');```

## Usage

Add each item to the bundle, to be saved later, by calling:

    $this->addToBundle($data);

Then, save the bundle to the database by calling:

    $this->saveBundle();

The bundle will save in chunks of 10,000 items by default, or you can pass in the max number of items to save per query:

    $this->saveBundle(1000);

If you are saving a row that already exists in the database, it will replace it by default. You can turn off replacement by passing ```false``` as the second argument

    $this->saveBundle(null, false);

## Authors

* Jarriett K. Robinson [jarriett (at) gmail.com] , http://github.com/jarriett
* Modifications by J. Miller, http://github.com/jmillerdesign

## License

Licensed under the MIT License Redistributions of files must retain the above copyright notice.
