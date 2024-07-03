# Spatio Temporal Accessibility Analysis of Public Transportation Networks

> :construction: **Work In Progress**: V 1.0 is expected to be deployed by 1st September 2024. Please **STAR** this repo to keep up-to-date.

The STAA framework provides a standardized pipeline for combining GTFS, OSM, census, and Point of Interest (POI) data into one coherent graph in space over time, ranging from days to years.

## Abstract from the Transit Data 2024 

Public transportation networks are the backbone of socio-economic functioning in the urban fabric, providing access to critical Points of Interest (POIs) such as school, work, and healthcare. However, there is a lack of a comprehensive spatio-temporal analysis framework to quantify and compare how network and service changes alter the accessibility for certain demographics of citizens. This work presents a reusable methodology to assess the impact of such operational modifications on the travel time to socio-economic POIs for different population groups. Using census, street, public transit network, and socio-economic data, we compute the correlation and statistical significance between travel time and population group membership based on income, education, or race. This approach allows transit planners to understand the effects of operational adaptions over time and space in hindsight and predict the outcomes of future modifications on access equity. We apply the presented methodology to the cities of Amsterdam, The Netherlands, and Atlanta, U.S.A., analyzing how changes to their transit networks during the COVID-19 pandemic affected access equity. We find that in Amsterdam, neighborhoods with higher percentages of individuals of non-western migration backgrounds experience higher travel times to schools using public transit. However, in Atlanta, individuals of similar demographics experience lower travel times using public transit. Finally, we provide the framework as an openly accessible Python package.

