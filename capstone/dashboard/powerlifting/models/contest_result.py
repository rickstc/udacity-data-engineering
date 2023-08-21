from django.db import models
from django.utils.translation import gettext_lazy as _


class ContestResult(models.Model):
    """
    TODO: Fill in information
    """

    class EventChoices(models.TextChoices):
        """
        A powerlifting contest focuses on three main lifts:
        - Back Squat
        - Bench Press
        - Deadlift

        Some contests allow the athletes to choose what type of "event" they want to
        participate in, which translates into what lifts the athlete performs during the
        contest.
        More information is here: https://openpowerlifting.gitlab.io/opl-csv/bulk-csv-docs.html
        """

        FULL_POWER = "FP", _("Squat-Bench-Deadlift")
        IRONMAN = "BD", _("Bench-Deadlift")
        SQUAT_DEADLIFT = "SD", _("Squat-Deadlift")
        SQUAT_BENCH = "SB", _("Squat-Bench")
        SQUAT_ONLY = "SQ", _("Squat")
        BENCH_ONLY = "BP", _("Bench")
        DEADLIFT_ONLY = "DL", _("Deadlift")

    class EquipmentChoices(models.TextChoices):
        """
        Different contests and federations allow different equipment.
        """

        RAW = "R", _("Raw (Bare knees or knee sleeves)")
        WRAPS = "W", _("Wraps (Knee wraps are allowed)")
        SINGLE_PLY = "S", _("Single-ply (Equipped, single-ply suits)")
        MULTI_PLY = "M", _("Multi-ply (Equipped, multi-ply suits)")
        UNLIMITED = "U", _("Unlimited (Equipped, multi-ply suits or rubberized gear)")
        STRAPS = "T", _("Straps (Allowed straps on the deadlift)")

    id = models.BigAutoField(primary_key=True)
    athlete = models.ForeignKey(
        "powerlifting.Athlete", related_name="results", on_delete=models.CASCADE
    )
    contest = models.ForeignKey(
        "powerlifting.Contest", related_name="results", on_delete=models.CASCADE
    )

    event = models.CharField(choices=EventChoices.choices, max_length=2)
    equipment = models.CharField(max_length=1, choices=EquipmentChoices.choices)
    drug_tested = models.BooleanField(default=False)
    division = models.CharField(max_length=32, blank=True, default="")

    age = models.DecimalField(max_digits=4, decimal_places=1, default=0)
    age_class = models.CharField(blank=True, default="", max_length=6)
    birth_year_class = models.CharField(blank=True, default="", max_length=6)

    bodyweight = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    weight_class = models.DecimalField(max_digits=5, decimal_places=2, default=0)

    deadlift = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    squat = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    bench_press = models.DecimalField(max_digits=5, decimal_places=2, default=0)

    place = models.PositiveSmallIntegerField(default=0)
    meet_total = models.DecimalField(max_digits=6, decimal_places=2, default=0)
    dots = models.DecimalField(max_digits=5, decimal_places=2, default=0)
