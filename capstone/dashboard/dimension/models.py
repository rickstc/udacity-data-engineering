from django.contrib.gis.db import models


class Contest(models.Model):
    date = models.DateField()
    name = models.CharField(max_length=256)
    federation = models.CharField(max_length=128)
    location = models.ForeignKey("dimension.Location", on_delete=models.CASCADE)
    average_dots = models.DecimalField(max_digits=5, decimal_places=2)
    standard_deviation = models.DecimalField(max_digits=5, decimal_places=2)


class Location(models.Model):
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
