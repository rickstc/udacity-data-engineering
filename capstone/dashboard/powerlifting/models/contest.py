from django.db import models
from django.utils.translation import gettext_lazy as _


class Contest(models.Model):
    """
    Represents a Powerlifting Meet (Contest)

    Fields:
    - ID
    - Event
    - Equipment
    """

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(blank=False, null=False, max_length=128)
    federation = models.CharField(max_length=64, blank=False, null=False)
    parent_federation = models.CharField(
        max_length=64, blank=True, null=False, default=""
    )
    date = models.DateField(blank=False, null=False)
    location = models.ForeignKey(
        "powerlifting.MeetLocation", blank=False, null=False, related_name="contests"
    )
