from django.contrib.gis.db import models


# Create your models here.
class Station(models.Model):
    id = models.BigAutoField(primary_key=True)
    station_id = models.CharField(unique=True, blank=False, max_length=11)
    elevation = models.DecimalField(max_digits=6, decimal_places=1)
    station_state = models.CharField(max_length=2, blank=True)
    station_name = models.CharField(max_length=30, blank=False)
    gsn_flag = models.BooleanField(default=False)
    wmo_id = models.PositiveIntegerField(default=0)
    station_location = models.PointField(srid=4326)


class CityLocation(models.Model):
    id = models.BigAutoField(primary_key=True)
    city = models.CharField(max_length=32)
    country = models.CharField(max_length=32)
    population = models.PositiveIntegerField(default=0)
    city_location = models.PointField(srid=4326)
