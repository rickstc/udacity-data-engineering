from rest_framework import serializers
from dimension import models

"""
Serializers are what Django Rest Framework uses to:

    "allow complex data such as querysets and model instances to be converted
    to native Python datatypes that can then be easily rendered into JSON, XML
    or other content types. Serializers also provide deserialization, allowing
    parsed data to be converted back into complex types, after first validating
    the incoming data."
    - https://www.django-rest-framework.org/api-guide/serializers/

The student won't go into detail on the usage of serializers in this project
as it is outside the scope of the assignment, with the exception of the
ReportingSerializer below.
"""


class ContestSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Contest
        fields = "__all__"


class LocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Location
        fields = "__all__"


class ReportingSerializer(serializers.ModelSerializer):
    """
    The ReportingSerializer defined here is the serializer that is used by the
    'report' endpoint. Declaring the serializer in this way essentially
    flattens the results (as opposed to nested by object) into a format more
    easily converted into a CSV.
    """

    country = serializers.CharField(source="location.country")
    state = serializers.CharField(source="location.state")
    town = serializers.CharField(source="location.town")
    weather_station = serializers.CharField(source="location.weather_station")
    distance_to_station = serializers.FloatField(source="location.distance_to_station")
    elevation = serializers.FloatField(source="location.elevation")
    population = serializers.IntegerField(source="location.population")

    class Meta:
        model = models.Contest
        fields = (
            "date",
            "name",
            "average_dots",
            "standard_deviation",
            "country",
            "state",
            "town",
            "weather_station",
            "distance_to_station",
            "elevation",
            "population",
        )
