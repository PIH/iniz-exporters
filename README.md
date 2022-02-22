# Iniz Exporters

Utilities to generate [Initializer](https://github.com/mekomsolutions/openmrs-module-initializer)-compatible CSVs from an OpenMRS database.

## concepts, locations

Requires Python 3.8

```
./setup.sh
./run.sh --help
```

### NB about locations exporter

It does *not* presently produce output which is sorted so that Initializer can read it in.
You must manually ensure that locations come *after* their parent locations.

### Notes about concepts exporter

To run the tests, you must have a local OpenMRS server.
Enter its info into `test_env.py`. Do not commit your changes to this file.
