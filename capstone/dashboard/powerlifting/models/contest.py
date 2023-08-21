from django.db import models
from django.utils.translation import gettext_lazy as _


class Contest(models.Model):
    """
    Represents a Powerlifting Meet (Contest)

    TODO: Fill in
    """

    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=256)
    federation = models.CharField(
        max_length=64,
    )
    parent_federation = models.CharField(max_length=64, blank=True, default="")
    date = models.DateField()
    location = models.ForeignKey(
        "powerlifting.ContestLocation",
        related_name="contests",
        on_delete=models.CASCADE,
    )
