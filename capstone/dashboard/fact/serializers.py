from rest_framework import serializers
from fact import models

"""
Serializers are what Django Rest Framework uses to:

    "allow complex data such as querysets and model instances to be converted
    to native Python datatypes that can then be easily rendered into JSON, XML
    or other content types. Serializers also provide deserialization, allowing
    parsed data to be converted back into complex types, after first validating
    the incoming data."
    - https://www.django-rest-framework.org/api-guide/serializers/

The student won't go into detail on the usage of serializers in this project
as it is outside the scope of the assignment.
"""


class ContestResultSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.ContestResult
        fields = "__all__"


class AthleteSerializer(serializers.ModelSerializer):
    # This will display an athlete's contest results with their data
    results = ContestResultSerializer(many=True)

    class Meta:
        model = models.Athlete
        fields = "__all__"


class ContestLocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.ContestLocation
        fields = "__all__"


class ContestSerializer(serializers.ModelSerializer):
    # This will display the location information of a given contest
    location = ContestLocationSerializer

    class Meta:
        model = models.Contest
        fields = "__all__"


class WeatherStationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.WeatherStation
        fields = "__all__"
