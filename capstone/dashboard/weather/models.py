from django.db import models


# Create your models here.
class Station(models.Model):
    station_id = models.CharField(unique=True, blank=False, max_length=11)
    elevation = models.DecimalField(max_digits=6, decimal_places=1)
    state = models.CharField(max_length=2, blank=True)
    name = models.CharField(max_length=30, blank=False)
    gsn_flag = models.BooleanField(default=False)
    wmo_id = models.PositiveIntegerField(default=0)
