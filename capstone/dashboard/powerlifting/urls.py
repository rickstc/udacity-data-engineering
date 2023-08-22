from rest_framework import routers
from powerlifting import viewsets

app_name = "powerlifting"

router = routers.DefaultRouter()
router.register(r"athletes", viewsets.AthleteViewSet, basename="athlete")
router.register(r"locations", viewsets.LocationViewSet, basename="location")
router.register(r"contests", viewsets.ContestViewSet, basename="contest")
router.register(r"results", viewsets.ContestResultViewSet, basename="result")
urlpatterns = router.urls
