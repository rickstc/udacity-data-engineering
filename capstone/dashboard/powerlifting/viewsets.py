from rest_framework import viewsets, mixins
from powerlifting import models, serializers


class AthleteViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Athlete.objects.all()
    serializer_class = serializers.AthleteSerializer
    filterset_fields = ["id", "name", "deduplication_number", "gender"]


class LocationViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.ContestLocation.objects.all()
    serializer_class = serializers.ContestLocationSerializer
    filterset_fields = ["id", "country", "state", "town"]


class ContestViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Contest.objects.all()
    serializer_class = serializers.ContestSerializer
    filterset_fields = [
        "id",
        "name",
        "federation",
        "parent_federation",
        "date",
        "location",
    ]


class ContestResultViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.ContestResult.objects.all()
    serializer_class = serializers.ContestResultSerializer
