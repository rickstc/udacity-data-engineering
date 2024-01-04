from rest_framework import serializers
from fact import models


class ContestResultSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.ContestResult
        fields = "__all__"


class AthleteSerializer(serializers.ModelSerializer):
    results = ContestResultSerializer(many=True)

    class Meta:
        model = models.Athlete
        fields = "__all__"


class ContestLocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.ContestLocation
        fields = "__all__"


class ContestSerializer(serializers.ModelSerializer):
    location = ContestLocationSerializer
    # results = ContestResultSerializer(many=True)

    class Meta:
        model = models.Contest
        fields = "__all__"


class WeatherStationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.WeatherStation
        fields = "__all__"
