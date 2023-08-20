from django.db import models


class Country(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=128, blank=False, null=False)


class State(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=128, blank=False, null=False)
    country = models.ForeignKey(Country, blank=False, null=False, related_name="states")


class Town(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=128, blank=False, null=False)
    country = models.ForeignKey(Country, blank=False, null=False, related_name="towns")


class ContestLocation(models.Model):
    id = models.BigAutoField(primary_key=True)
    country = models.ForeignKey(
        Country, blank=False, null=False, related_name="locations"
    )
    state = models.ForeignKey(State, blank=True, null=True, related_name="locations")
    town = models.ForeignKey(Town, blank=True, null=True, related_name="locations")
