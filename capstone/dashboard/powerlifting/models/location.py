from django.db import models

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


class ContestLocation(models.Model):
    id = models.BigAutoField(primary_key=True)
    country = models.CharField(max_length=128)
    state = models.CharField(max_length=128, null=False, blank=True, default="")
    town = models.CharField(max_length=128, null=False, blank=True, default="")

    class Meta:
        unique_together = ("country", "state", "town")

    def __str__(self) -> str:
        return f"{self.town} {self.state} {self.country}"
