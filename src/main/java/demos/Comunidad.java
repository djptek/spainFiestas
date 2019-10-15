package demos;

import java.text.Normalizer;

enum Comunidad {
    ANDALUCIA("ANDALUCÍA", "ES-AN", -5.98, 37.37),
    ARAGON("ARAGÓN", "ES-AR", -0.88, 41.66),
    ASTURIAS("ASTURIAS", "ES-AS", -5.85, 43.36),
    CANARIAS("CANARIAS", "ES-CN", -15.42, 28.15),
    CANTABRIA("CANTABRIA", "ES-CB", -3.80, 43.45),
    CASTILLA_Y_LEON("CASTILLA Y LEÓN", "ES-CL", -4.72, 41.63),
    CASTILLA_LA_MANCHA("CASTILLA-LA MANCHA", "ES-CM", -4.03, 39.86),
    CATALUNA("CATALUÑA", "ES-CT", 2.17, 41.38),
    CIUDAD_DE_CEUTA("CIUDAD DE CEUTA", "ES-CE", -5.32, 35.89),
    CIUDAD_DE_MELILLA("CIUDAD DE MELILLA", "ES-ML", -2.94, 35.29),
    COMUNITAT_VALENCIANA("COMUNITAT VALENCIANA", "ES-VC", -0.38, 39.47),
    EXTREMADURA("EXTREMADURA", "ES-EX", -6.33, 38.90),
    GALICIA("GALICIA", "ES-GA", -8.55, 42.87),
    ILLES_BALEARS("ILLES BALEARS", "ES-IB", -2.65, 39.57),
    LA_RIOJA("LA RIOJA", "ES-RI", -2.45, 42.46),
    MADRID("MADRID", "ES-MD", -3.68, 40.43),
    MURCIA("MURCIA", "ES-MC", -1.13, 37.99),
    NAVARRA("NAVARRA", "ES-NC", -1.64, 42.82),
    PAIS_VASCO("PAÍS VASCO", "ES-PV", -2.68, 42.85);

    public final String userLevel;
    public final String isocode;
    public final double lon;
    public final double lat;

    public final static int MAX_COMUNIDADES = Comunidad.values().length;

    Comunidad(String userLevel, String isocode, double lon, double lat) {
        this.userLevel = userLevel;
        this.isocode = isocode;
        this.lon = lon;
        this.lat = lat;
    }

    // Map accented charaters to a normalized version suitable for use as enum

    public static Comunidad valueOfNormalized(String input) {
        return valueOf(Normalizer.normalize(input,
                Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", "")
                .replaceAll("[ -]", "_"));
    }

}
