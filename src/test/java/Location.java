import java.util.Random;

public class Location {
    private String name;

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    private double longitude;

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    private double latitude;

    // create and initialize a point with given name and
    // (latitude, longitude) specified in degrees
    public Location(String name, double latitude, double longitude) {
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Location(Location location) {
        this.name = location.name;
        this.latitude = location.getLatitude();
        this.longitude = location.getLongitude();
    }

    // return distance between this location and that location
    // measured in statute miles
    public double distanceTo(Location that) {
        double STATUTE_MILES_PER_NAUTICAL_MILE = 1.15077945;
        double lat1 = Math.toRadians(this.latitude);
        double lon1 = Math.toRadians(this.longitude);
        double lat2 = Math.toRadians(that.latitude);
        double lon2 = Math.toRadians(that.longitude);

        // great circle distance in radians, using law of cosines formula
        double angle = Math.acos(Math.sin(lat1) * Math.sin(lat2)
                + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon1 - lon2));

        // each degree on a great circle of Earth is 60 nautical miles
        double nauticalMiles = 60 * Math.toDegrees(angle);
        double statuteMiles = STATUTE_MILES_PER_NAUTICAL_MILE * nauticalMiles;
        return statuteMiles;
    }

    // return string representation of this point
    public String toString() {
        return name + " (" + latitude + ", " + longitude + ")";
    }

    protected static Location getLocationInLatLngRad(double radiusInMeters, Location currentLocation) {
        double x0 = currentLocation.getLongitude();
        double y0 = currentLocation.getLatitude();

        Random random = new Random();

        // Convert radius from meters to degrees.
        double radiusInDegrees = radiusInMeters / 111320f;

        // Get a random distance and a random angle.
        double u = random.nextDouble();
        double v = random.nextDouble();
        double w = radiusInDegrees * Math.sqrt(u);
        double t = 2 * Math.PI * v;
        // Get the x and y delta values.
        double x = w * Math.cos(t);
        double y = w * Math.sin(t);

        // Compensate the x value.
        double new_x = x / Math.cos(Math.toRadians(y0));

        double foundLatitude;
        double foundLongitude;

        foundLatitude = y0 + y;
        foundLongitude = x0 + new_x;

        Location copy = new Location(currentLocation);
        copy.setLatitude(foundLatitude);
        copy.setLongitude(foundLongitude);
        return copy;
    }


    // test client
    public static void main(String[] args) {
        Location loc3 = new Location("Bangalore", 12.9716, 77.5946);
        Location loc4 = getLocationInLatLngRad(5000,loc3);

        System.out.println(getLocationInLatLngRad(10000,loc3));

        double distance = loc3.distanceTo(loc4);
        System.out.printf("%6.3f miles from\n", distance);
        System.out.println(loc3 + " to " + loc4);

    }
}
