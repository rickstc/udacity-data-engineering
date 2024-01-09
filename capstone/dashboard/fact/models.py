from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _


class Athlete(models.Model):
    """
    Represents an Athlete participating in a competition.

    Fields:
    id - Unique integer field to uniquely identify an athlete
    name - The athlete's name
    deduplication_number - A number provided to distinguish between athletes
        with the same name. According to the OPL documentation, two lifters
        with the same name would have a number symbol followed by a unique
        number.
        EX: John Doe #1 and John Doe #2.

        The pipeline will need to detect this and perform the insertions
        correctly, and this column will help in doing that.
    gender - The gender category that the lifter competed in. See the
        GenderChoices subclass below for more information
    """

    class GenderChoices(models.TextChoices):
        """
        The choices class defines the acceptable values that can be stored
        on a field that selects it as the choices. In this instance, the gender field
        will be restricted to one of these values.

        According to the OPL documentation, Mx is a gender-neutral title originating in
        the UK. It serves as a catch-all sex category that is particularly appropriate
        for non-binary athletes.
        """

        MALE = "M", _("Male")
        FEMALE = "F", _("Female")
        MX = "X", _("Mx")

    # Fields
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=128)
    deduplication_number = models.PositiveSmallIntegerField(default=0)
    gender = models.CharField(
        max_length=1,
        choices=GenderChoices.choices,
        default=GenderChoices.MX,
    )

    # Meta Class
    class Meta:
        """
        The Model's Metadata class defines options such as ordering, table names, etc.
        """

        # This defines a database constraint that forces the name and deduplication_number
        # to be unique when taken together
        unique_together = ("name", "deduplication_number")


class Contest(models.Model):
    """
    Represents a Powerlifting Meet (Contest)
    """

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=256)
    federation = models.CharField(
        max_length=64,
    )
    parent_federation = models.CharField(max_length=64, blank=True, default="")
    date = models.DateField()
    location = models.ForeignKey(
        "fact.ContestLocation",
        related_name="contests",
        on_delete=models.CASCADE,
    )


class ContestResult(models.Model):
    """
    Represents a contest result
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
        "fact.Athlete", related_name="results", on_delete=models.CASCADE
    )
    contest = models.ForeignKey(
        "fact.Contest", related_name="results", on_delete=models.CASCADE
    )

    event = models.CharField(choices=EventChoices.choices, max_length=2)
    equipment = models.CharField(max_length=1, choices=EquipmentChoices.choices)
    drug_tested = models.BooleanField(default=False)
    division = models.CharField(max_length=64, blank=True, default="")

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


class ContestLocation(models.Model):
    """
    Deciding how to store Country, State, and Town was somewhat tricky. Based on what we can
    infer from the documentation and the data, there are three fields that we can use to
    determine the location that the meet (contest) took place:
    - MeetCountry - Required
    - MeetState - Optional - References a state, province, or region.
    - MeetTown - Not in documentation, but based on the data, also optional

    In an ideal world, we might have a table each for Country, State, and City, where state
    references Country, and City references State, and a "ContestLocation" table that
    references the city where the meet took place. However, the data does not lend itself to
    that structure, as both State and/or City can be blank.

    The format I've settled on is a single table; ContestLocation, with the following fields:
        - id
        - country - Required
        - state - Not required
        - town - Not required

    I've also defined a unique_together constraint to ensure that country, state, and town,
    when taken together, are unique. This will prevent duplicate entries based on what is
    actually provided, while allowing for missing data points on the actual individual fields.
    """

    id = models.BigAutoField(primary_key=True)
    country = models.CharField(max_length=128)
    state = models.CharField(max_length=128, null=False, blank=True, default="")
    town = models.CharField(max_length=128, null=False, blank=True, default="")
    location = models.PointField(blank=True, null=True, default=None, srid=4326)
    population = models.PositiveIntegerField(blank=False, default=0)

    class Meta:
        unique_together = ("country", "state", "town")

    def __str__(self) -> str:
        return f"{self.town} {self.state} {self.country}"


class WeatherStation(models.Model):
    """
    Represents a weather station
    """
    id = models.BigAutoField(primary_key=True)
    station_id = models.CharField(unique=True, blank=False, max_length=11)
    elevation = models.DecimalField(max_digits=6, decimal_places=1)
    station_state = models.CharField(max_length=2, blank=True)
    station_name = models.CharField(max_length=30, blank=False)
    gsn_flag = models.BooleanField(default=False)
    wmo_id = models.PositiveIntegerField(default=0)
    station_location = models.PointField(srid=4326)
