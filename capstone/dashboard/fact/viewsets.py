from rest_framework import viewsets, mixins
from fact import models, serializers

"""
ViewSets are what Django Rest Framework uses to:
    Django REST framework allows you to combine the logic for a set of related
    views in a single class, called a ViewSet. In other frameworks you may also
    find conceptually similar implementations named something like 'Resources'
    or 'Controllers'.

    A ViewSet class is simply a type of class-based View, that does not provide
    any method handlers such as .get() or .post(), and instead provides actions
    such as .list() and .create().
    https://www.django-rest-framework.org/api-guide/viewsets/

The student won't go into detail on the usage of ViewSets in this project as it
is outside the scope of the assignment.
"""


class AthleteViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Athlete.objects.all()
    serializer_class = serializers.AthleteSerializer

    # This allows the user to filter the results based on the following fields
    filterset_fields = ["id", "name", "deduplication_number", "gender"]


class LocationViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.ContestLocation.objects.all()
    serializer_class = serializers.ContestLocationSerializer

    # This allows the user to filter the results based on the following fields
    filterset_fields = ["id", "country", "state", "town"]


class ContestViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Contest.objects.all()
    serializer_class = serializers.ContestSerializer

    # This allows the user to filter the results based on the following fields
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


class StationViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.WeatherStation.objects.all()
    serializer_class = serializers.WeatherStationSerializer
