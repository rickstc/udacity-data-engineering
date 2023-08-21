from rest_framework import serializers
from powerlifting import models


class AthleteSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Athlete
        fields = "__all__"


class ContestLocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.ContestLocation
        fields = "__all__"


class ContestSerializer(serializers.ModelSerializer):
    location = ContestLocationSerializer

    class Meta:
        model = models.Contest
        fields = "__all__"
