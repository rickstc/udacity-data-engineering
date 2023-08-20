from django.db import models


class ContestResult(models.Model):
    """
    TODO:
    event
    equipment
    age
    age_class
    birth_year_class
    division
    bodyweight
    weight_class
    place
    dots
    wilks
    glossbrenner
    goodlift
    location
    """

    id = models.BigAutoField(primary_key=True)
    athlete = models.ForeignKey(
        "powerlifting.Athlete", blank=False, null=False, related_name="results"
    )
    contest = models.ForeignKey(
        "powerlifting.Contest", blank=False, null=False, related_name="results"
    )
    deadlift = models.DecimalField(max_digits=5, decimal_places=2)
    squat = models.DecimalField(max_digits=5, decimal_places=2)
    bench_press = models.DecimalField(max_digits=5, decimal_places=2)
    meet_total = models.DecimalField(max_digits=6, decimal_palaces=2)
    drug_tested = models.BooleanField(default=False)
