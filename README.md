# Thames Water EDM API fetcher
This python script will connect to the Thames Water EDM API (https://data.thameswater.co.uk/s/) to retrieve data relating to Storm Discharge events at specific monitoring stations.
Data is saved in csv format for viewing in your preferred spreadsheet app.

For more information about Storm Discharge and EDM (Event Data Monitoring) see:
* [thameswater.co.uk > EDM map (to find stations you're interested in)](https://www.thameswater.co.uk/edm-map)
* [thameswater.co.uk > EDM data info](https://www.thameswater.co.uk/about-us/performance/river-health/storm-discharge-data)
* [thameswater.co.uk > Storm Discharge FAQs](https://www.thameswater.co.uk/about-us/performance/river-health/frequently-asked-questions/information-about-storm-discharge)

## Prerequesites
* Register on the Thames Water data site for your API credentials and add to `river_secrets.py`
* Python 3
* Requests library
* Pandas library
