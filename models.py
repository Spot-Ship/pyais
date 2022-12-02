from django.db import models
import datetime

class Orbcomm_Vessels(models.Model):
    mmsi = models.CharField(max_length=10, blank=False, null=False, unique=True, primary_key=True,
        help_text="Maritime Mobile Service Identity (MMSI) 9-digit identifier")
    imo = models.CharField(max_length=10, blank=True, null=True,
        help_text="International Maritime Organization (IMO) 7-digit identifier"
        """
        0 = not available = default – Not applicable to SAR aircraft
        0000000001-0000999999 not used
        0001000000-0009999999 = valid IMO number;
        0010000000-1073741823 = official flag state number.
        """)
    vessel_name = models.CharField(max_length=100, blank=False, null=False, unique=False,
        help_text="Vessel name. Not unique.")
    vessel_callsign = models.CharField(max_length=100, blank=False, null=False, unique=False,
        help_text="Vessel callsign. Not unique.")
    vessel_type = models.CharField(max_length=100, blank=True, null=True, unique=False,
        help_text="VESSEL TYPE DESCRIPTION"
        """
        0 = not available or no ship = default
        1-99 = as defined below
        100-199 = reserved, for regional use
        200-255 = reserved, for future use
        """)
    destination =  models.CharField(max_length=100, blank=True, null=True, unique=False,
        help_text="Maximum 20 characters using 6-bit ASCII.")
    eta =  models.DateTimeField(blank=True, null=True, unique=False,
        help_text="DTE"
        """
        Estimated time of arrival; MMDDHHMM UTC
        Bits 19-16: month; 1-12; 0 = not available = default
        Bits 15-11: day; 1-31; 0 = not available = default
        Bits 10-6: hour; 0-23; 24 = not available = default
        Bits 5-0: minute; 0-59; 60 = not available = default
        """)
    draught = models.IntegerField(blank=True, null=True, unique=False,
        help_text="Vessel draft in metres")
    to_bow = models.IntegerField(blank=True, null=True, unique=False,
        help_text="Vessel dimensions in metres")
    to_stern = models.IntegerField(blank=True, null=True, unique=False,
        help_text="Vessel dimensions in metres")
    to_port = models.IntegerField(blank=True, null=True, unique=False,
        help_text="Vessel dimensions in metres")
    to_starboard = models.IntegerField(blank=True, null=True, unique=False,
        help_text="Vessel dimensions in metres")

    spotship_id = models.CharField(max_length=128, blank=True, null=True, unique=True, 
        help_text="Spotship id to update positions.")
    is_ais_enabled= models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True, blank=True)
    position_updated_at = models.DateTimeField(auto_now_add=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True)

    def __str__(self):
        try:
            return str(self.mmsi) + " "+ str(self.vessel_name) + " " + str(self.vessel_type)
        except:
            return str(self.mmsi)     

class Orbcomm_Positions(models.Model):
    mmsi = models.ForeignKey(Orbcomm_Vessels, on_delete=models.PROTECT, db_column='mmsi',
        help_text="Maritime Mobile Service Identity (MMSI) 9-digit identifier")
    lon = models.DecimalField(max_digits=16, decimal_places=13, blank=True, null=True,
        help_text="Longitude in 1/10 000 min (+/-180 deg, East = positive (as per 2's complement), West = negative (as per 2's complement). 181= (6791AC0h) = not available = default)")
    lat = models.DecimalField(max_digits=16 , decimal_places=13, blank=True, null=True,
        help_text="Latitude in 1/10 000 min (+/-90 deg, North = positive (as per 2's complement), South = negative (as per 2's complement). 91deg (3412140h) = not available = default)")
    course = models.DecimalField(max_digits=16 , decimal_places=5, blank=True, null=True,
        help_text="Course over ground (COG) in 1/10 = (0-3599). 3600 (E10h) = not available = default. 3 601-4 095 should not be used")
    heading =  models.IntegerField(blank=True, null=True, unique=False,
        help_text="TRUE HEADING. Degrees (0-359) (511 indicates not available = default)")
    status = models.CharField(max_length=100, blank=True, null=True, unique=False,
        help_text="STATUS DESCRIPTION")
    accuracy = models.IntegerField(blank=True, null=True, unique=False,
        help_text="ACCURACY DESCRIPTION"
        """  	
        The position accuracy (PA) flag should be determined in accordance with the table below:
        1 = high (<= 10 m)
        0 = low (> 10 m)
        0 = default
        """)
    speed =  models.DecimalField(max_digits=16 , decimal_places=5, blank=True, null=True,
        help_text="Speed over ground in 1/10 knot steps (0-102.2 knots), 1 023 = not available, 1 022 = 102.2 knots or higher")

    created_at = models.DateTimeField(auto_now_add=True, blank=True)


    def __str__(self):
        return str(self.mmsi)



