from django.db import models

class LocationUpdate(models.Model):
    latitude = models.FloatField()
    longitude = models.FloatField()
    time = models.DateTimeField(auto_now_add=True)
