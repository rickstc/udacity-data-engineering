from rest_framework import routers
from fact import viewsets

app_name = "fact"

router = routers.DefaultRouter()
router.register(r"athletes", viewsets.AthleteViewSet, basename="athlete")
router.register(r"locations", viewsets.LocationViewSet, basename="location")
router.register(r"contests", viewsets.ContestViewSet, basename="contest")
router.register(r"results", viewsets.ContestResultViewSet, basename="result")
router.register(r"stations", viewsets.StationViewSet, basename="station")
urlpatterns = router.urls
