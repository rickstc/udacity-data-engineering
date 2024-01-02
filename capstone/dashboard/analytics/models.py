from django.db import models


class Contest(models.Model):
    date = models.DateField()
    name = models.CharField(max_length=256)
    federation = models.CharField(max_length=128)
    country = models.CharField(max_length=128)
    city_name = models.CharField(max_length=128, blank=True, null=True)


class Location(models.Model):
    contest = models.ForeignKey(Contest, on_delete=models.CASCADE)
    station_name = models.CharField(max_length=30, blank=True, null=True)
    elevation = models.DecimalField(
        max_digits=6, decimal_places=1, blank=True, null=True
    )
    population = models.BigIntegerField(blank=True, null=True)
    distance_to_station = models.DecimalField(
        max_digits=6, decimal_places=1, blank=True, null=True
    )


class ScoreDistribution(models.Model):
    contest = models.ForeignKey(Contest, on_delete=models.CASCADE)
    average_dots = models.DecimalField(max_digits=5, decimal_places=2)
    standard_deviation = models.DecimalField(max_digits=5, decimal_places=2)
    # Add additional fields for other percentiles or statistical measures


# Elevation

# Population
