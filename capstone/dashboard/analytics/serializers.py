from rest_framework import serializers
from analytics import models


class ContestSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Contest
        fields = "__all__"
