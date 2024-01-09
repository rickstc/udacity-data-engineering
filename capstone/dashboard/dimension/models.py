from django.contrib.gis.db import models

"""
The student defined the models in this file based on the structure of
the data and the desired functionality of the application.

The purpose of the 'dimension' models is to provide information to data users
to accomplish the goal of analyzing contest results in the context of the
location where the contest took place.
"""


class Contest(models.Model):
    """
    The contest model stores a reference to the fact contest's primary key,
    basic information about the contest such as date, name, federation,
    the foreign key to the location table, and calculated information from the
    fact contest results table; specifically the average DOTS scores of all
    participants at that contest as well as the standard deviation of those
    scores.
    """

    fact_contest_id = models.BigIntegerField(unique=True)
    date = models.DateField()
    name = models.CharField(max_length=256)
    federation = models.CharField(max_length=128)
    location = models.ForeignKey("dimension.Location", on_delete=models.CASCADE)
    average_dots = models.DecimalField(max_digits=5, decimal_places=2)
    standard_deviation = models.DecimalField(max_digits=5, decimal_places=2)


class Location(models.Model):
    """
    The location model stores a reference to the fact location's primary key,
    basic information about the location of the contest such as country, state,
    town and population.

    Also contained is information about the nearest weather station, including
    the station's name, coordinates, the distance (in km) to the weather
    station, and the elevation of the station.
    """

    fact_location_id = models.BigIntegerField(unique=True)
    country = models.CharField(max_length=128)
    state = models.CharField(max_length=128, null=False, blank=True, default="")
    town = models.CharField(max_length=128, null=False, blank=True, default="")
    loc_coords = models.PointField(srid=4326)
    weather_station = models.CharField(max_length=30, blank=False)
    station_coords = models.PointField(srid=4326)

    distance_to_station = models.DecimalField(
        max_digits=6, decimal_places=1, blank=True, null=True
    )

    elevation = models.DecimalField(
        max_digits=6, decimal_places=1, blank=True, null=True
    )
    population = models.PositiveIntegerField(blank=False, default=0)
