from rest_framework import viewsets, mixins
from dimension import models, serializers
from rest_framework.decorators import action
from rest_framework.response import Response


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


class ContestViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = models.Contest.objects.all()

    def get_serializer_class(self):
        # Special serializer for the 'report' endpoint
        if self.action == "report":
            return serializers.ReportingSerializer

        # Default serializer for all requests (there is only list)
        return serializers.ContestSerializer

    @action(methods=["GET"], detail=False)
    def report(self, request, pk=None):
        """
        An endpoint for generating a report of contests; bypasses pagination
        and returns a flat list suitable to ease parsing the CSV/JSON/XLS
        """

        queryset = self.filter_queryset(
            self.get_queryset().prefetch_related("location")
        )

        serializer = self.get_serializer(queryset, many=True)

        return Response(serializer.data)


class LocationViewSet(viewsets.GenericViewSet, mixins.ListModelMixin):
    ordering_fields = ["elevation", "distance_to_station"]
    queryset = models.Location.objects.all()
    serializer_class = serializers.LocationSerializer
