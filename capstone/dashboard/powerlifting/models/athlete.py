from django.db import models
from django.utils.translation import gettext_lazy as _


class Athlete(models.Model):
    """
    Represents an Athlete participating in a competition.

    Fields:
    id - Unique integer field to uniquely identify an athlete
    name - The athlete's name
    deduplication_number - A number provided to distinguish between athletes with the
        same name. According to the OPL documentation, two lifters with the same name
        would have a number symbol followed by a unique number.
        EX: John Doe #1 and John Doe #2.

        The pipeline will need to detect this and perform the insertions correctly, and
        this column will help in doing that.
    gender - The gender category that the lifter competed in. See the GenderChoices
        subclass below for more information
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
    name = models.CharField(blank=False, null=False, max_length=128)
    deduplication_number = models.PositiveSmallIntegerField(
        blank=False, null=False, default=0
    )
    gender = models.CharField(
        blank=False,
        null=False,
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
