from rest_framework import serializers
from dimension import models


class ContestSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Contest
        fields = "__all__"


class LocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Location
        fields = "__all__"
