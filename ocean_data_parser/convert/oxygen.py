"""
Converstion of the matlab package SCOR WG 142 to python

SCOR WG 142: Quality Control Procedures for Oxygen and Other Biogeochemical Sensors on Floats and Gliders. Recommendations on the conversion between oxygen quantities for Bio-Argo floats and other autonomous sensor platforms.
https://archimer.ifremer.fr/doc/00348/45915/
DOI 	10.13155/45915
"""

import numpy as np

Vm = 0.317  # molar volume of O2 in m3 mol-1 Pa dbar-1 (Enns et al. 1965)
R = 8.314  # universal gas constant in J mol-1 K-1


def O2ctoO2p(O2conc, T, S, P=0):
    # function pO2=O2ctoO2p(O2conc,temp,sal,pres)
    #
    # convert molar oxygen concentration to oxygen partial pressure
    #
    # inputs:
    #   O2conc - oxygen concentration in umol L-1
    #   T      - temperature in °C
    #   S      - salinity (PSS-78)
    #   P      - hydrostatic pressure in dbar (default: 0 dbar)
    #
    # output:
    #   pO2    - oxygen partial pressure in mbar
    #
    # according to recommendations by SCOR WG 142 "Quality Control Procedures
    # for Oxygen and Other Biogeochemical Sensors on Floats and Gliders"
    #
    # Henry Bittig
    # Laboratoire d'Océanographie de Villefranche-sur-Mer, France
    # bittig@obs-vlfr.fr
    # 28.10.2015
    # 19.04.2018, v1.1, fixed typo in B2 np.exponent
    # 01.06.2022, Converted to python

    xO2 = 0.20946  # mole fraction of O2 in dry air (Glueckauf 1951)
    pH2Osat = 1013.25 * (
        np.exp(
            24.4543
            - (67.4509 * (100 / (T + 273.15)))
            - (4.8489 * np.log(((273.15 + T) / 100)))
            - 0.000544 * S
        )
    )  # saturated water vapor in mbar
    sca_T = np.log(
        (298.15 - T) / (273.15 + T)
    )  # scaled temperature for use in TCorr and SCorr
    TCorr = 44.6596 * np.exp(
        2.00907
        + 3.22014 * sca_T
        + 4.05010 * sca_T**2
        + 4.94457 * sca_T**3
        - 2.56847e-1 * sca_T**4
        + 3.88767 * sca_T**5
    )  # temperature correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit mL(STP) L-1 and conversion from mL(STP) L-1 to umol L-1
    Scorr = np.exp(
        S
        * (
            -6.24523e-3
            - 7.37614e-3 * sca_T
            - 1.03410e-2 * sca_T**2
            - 8.17083e-3 * sca_T**3
        )
        - 4.88682e-7 * S**2
    )  # salinity correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit ml(STP) L-1

    return (
        O2conc
        * (xO2 * (1013.25 - pH2Osat))
        / (TCorr * Scorr)
        * np.exp(Vm * P / (R * (T + 273.15)))
    )


def O2ctoO2s(O2conc, T, S, P=0, p_atm=1013.25):
    # O2ctoO2s(O2conc,T,S,P,p_atm)
    #
    # convert molar oxygen concentration to oxygen saturation
    #
    # inputs:
    #   O2conc - oxygen concentration in umol L-1
    #   T      - temperature in ∞C
    #   S      - salinity (PSS-78)
    #   P      - hydrostatic pressure in dbar (default: 0 dbar)
    #   p_atm  - atmospheric (air) pressure in mbar (default: 1013.25 mbar)
    #
    # output:
    #   O2sat  - oxygen saturation in #
    #
    # according to recommendations by SCOR WG 142 "Quality Control Procedures
    # for Oxygen and Other Biogeochemical Sensors on Floats and Gliders"
    #
    # Henry Bittig
    # Laboratoire d'OcÈanographie de Villefranche-sur-Mer, France
    # bittig@obs-vlfr.fr
    # 28.10.2015
    # 19.04.2018, v1.1, fixed typo in B2 np.exponent
    # 01.06.2022, Converted to python

    pH2Osat = 1013.25 * (
        np.exp(
            24.4543
            - (67.4509 * (100 / (T + 273.15)))
            - (4.8489 * np.log(((273.15 + T) / 100)))
            - 0.000544 * S
        )
    )  # saturated water vapor in mbar
    sca_T = np.log(
        (298.15 - T) / (273.15 + T)
    )  # scaled temperature for use in TCorr and SCorr
    TCorr = 44.6596 * np.exp(
        2.00907
        + 3.22014 * sca_T
        + 4.05010 * sca_T**2
        + 4.94457 * sca_T**3
        - 2.56847e-1 * sca_T**4
        + 3.88767 * sca_T**5
    )  # temperature correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit mL(STP) L-1 and conversion from mL(STP) L-1 to umol L-1
    Scorr = np.exp(
        S
        * (
            -6.24523e-3
            - 7.37614e-3 * sca_T
            - 1.03410e-2 * sca_T**2
            - 8.17083e-3 * sca_T**3
        )
        - 4.88682e-7 * S**2
    )  # salinity correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit ml(STP) L-1

    return (
        O2conc
        * 100
        / (TCorr * Scorr)
        / (p_atm - pH2Osat)
        * (1013.25 - pH2Osat)
        * np.exp(Vm * P / (R * (T + 273.15)))
    )


def O2ptoO2c(pO2, T, S, P=0):
    # O2ptoO2c(pO2,T,S,P)
    #
    # convert oxygen partial pressure to molar oxygen concentration
    #
    # inputs:
    #   pO2    - oxygen partial pressure in mbar
    #   T      - temperature in °C
    #   S      - salinity (PSS-78)
    #   P      - hydrostatic pressure in dbar (default: 0 dbar)
    #
    # output:
    #   O2conc - oxygen concentration in umol L-1
    #
    # according to recommendations by SCOR WG 142 "Quality Control Procedures
    # for Oxygen and Other Biogeochemical Sensors on Floats and Gliders"
    #
    # Henry Bittig
    # Laboratoire d'Océanographie de Villefranche-sur-Mer, France
    # bittig@obs-vlfr.fr
    # 28.10.2015
    # 19.04.2018, v1.1, fixed typo in B2 np.exponent
    # 01.06.2022, Converted to python

    xO2 = 0.20946  # mole fraction of O2 in dry air (Glueckauf 1951)
    pH2Osat = 1013.25 * (
        np.exp(
            24.4543
            - (67.4509 * (100 / (T + 273.15)))
            - (4.8489 * np.log(((273.15 + T) / 100)))
            - 0.000544 * S
        )
    )  # saturated water vapor in mbar
    sca_T = np.log(
        (298.15 - T) / (273.15 + T)
    )  # scaled temperature for use in TCorr and SCorr
    TCorr = 44.6596 * np.exp(
        2.00907
        + 3.22014 * sca_T
        + 4.05010 * sca_T**2
        + 4.94457 * sca_T**3
        - 2.56847e-1 * sca_T**4
        + 3.88767 * sca_T**5
    )  # temperature correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit mL(STP) L-1 and conversion from mL(STP) L-1 to umol L-1
    Scorr = np.exp(
        S
        * (
            -6.24523e-3
            - 7.37614e-3 * sca_T
            - 1.03410e-2 * sca_T**2
            - 8.17083e-3 * sca_T**3
        )
        - 4.88682e-7 * S**2
    )  # salinity correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit ml(STP) L-1

    return (
        pO2
        / (xO2 * (1013.25 - pH2Osat))
        * (TCorr * Scorr)
        / np.exp(Vm * P / (R * (T + 273.15)))
    )


def O2ptoO2s(pO2, T, S, P=0, p_atm=1013.25):
    # function O2sat=O2ptoO2s(pO2,T,S,P,p_atm)
    #
    # convert oxygen partial pressure to oxygen saturation
    #
    # inputs:
    #   pO2    - oxygen partial pressure in mbar
    #   T      - temperature in °C
    #   S      - salinity (PSS-78)
    #   P      - hydrostatic pressure in dbar (default: 0 dbar)
    #   p_atm  - atmospheric (air) pressure in mbar (default: 1013.25 mbar)
    #
    # output:
    #   O2sat  - oxygen saturation in #
    #
    # according to recommendations by SCOR WG 142 "Quality Control Procedures
    # for Oxygen and Other Biogeochemical Sensors on Floats and Gliders"
    #
    # Henry Bittig
    # Laboratoire d'Océanographie de Villefranche-sur-Mer, France
    # bittig@obs-vlfr.fr
    # 28.10.2015
    # 01.06.2022, Converted to python

    xO2 = 0.20946  # mole fraction of O2 in dry air (Glueckauf 1951)
    pH2Osat = 1013.25 * (
        np.exp(
            24.4543
            - (67.4509 * (100 / (T + 273.15)))
            - (4.8489 * np.log(((273.15 + T) / 100)))
            - 0.000544 * S
        )
    )  # saturated water vapor in mbar

    return pO2 * 100 / (xO2 * (p_atm - pH2Osat))


def O2stoO2c(O2sat, T, S, P=0, p_atm=1013.25):
    # function O2conc=O2stoO2c(O2sat,T,S,P,p_atm)
    #
    # convert oxygen saturation to molar oxygen concentration
    #
    # inputs:
    #   O2sat  - oxygen saturation in %
    #   T      - temperature in °C
    #   S      - salinity (PSS-78)
    #   P      - hydrostatic pressure in dbar (default: 0 dbar)
    #   p_atm  - atmospheric (air) pressure in mbar (default: 1013.25 mbar)
    #
    # output:
    #   O2conc - oxygen concentration in umol L-1
    #
    # according to recommendations by SCOR WG 142 "Quality Control Procedures
    # for Oxygen and Other Biogeochemical Sensors on Floats and Gliders"
    #
    # Henry Bittig
    # Laboratoire d'Océanographie de Villefranche-sur-Mer, France
    # bittig@obs-vlfr.fr
    # 28.10.2015
    # 19.04.2018, v1.1, fixed typo in B2 np.exponent
    # 01.06.2022, Converted to python

    pH2Osat = 1013.25 * (
        np.exp(
            24.4543
            - (67.4509 * (100 / (T + 273.15)))
            - (4.8489 * np.log(((273.15 + T) / 100)))
            - 0.000544 * S
        )
    )  # saturated water vapor in mbar
    sca_T = np.log(
        (298.15 - T) / (273.15 + T)
    )  # scaled temperature for use in TCorr and SCorr
    TCorr = 44.6596 * np.exp(
        2.00907
        + 3.22014 * sca_T
        + 4.05010 * sca_T**2
        + 4.94457 * sca_T**3
        - 2.56847e-1 * sca_T**4
        + 3.88767 * sca_T**5
    )  # temperature correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit mL(STP) L-1 and conversion from mL(STP) L-1 to umol L-1
    Scorr = np.exp(
        S
        * (
            -6.24523e-3
            - 7.37614e-3 * sca_T
            - 1.03410e-2 * sca_T**2
            - 8.17083e-3 * sca_T**3
        )
        - 4.88682e-7 * S**2
    )  # salinity correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit ml(STP) L-1

    return (
        O2sat
        / 100
        * (TCorr * Scorr)
        * (p_atm - pH2Osat)
        / (1013.25 - pH2Osat)
        / np.exp(Vm * P / (R * (T + 273.15)))
    )


def O2stoO2p(O2sat, T, S, P=0, p_atm=1013.25):
    # O2stoO2p(O2sat,T,S,P,p_atm)
    #
    # convert oxygen saturation to oxygen partial pressure
    #
    # inputs:
    #   O2sat  - oxygen saturation in #
    #   T      - temperature in °C
    #   S      - salinity (PSS-78)
    #   P      - hydrostatic pressure in dbar (default: 0 dbar)
    #   p_atm  - atmospheric (air) pressure in mbar (default: 1013.25 mbar)
    #
    # output:
    #   pO2    - oxygen partial pressure in mbar
    #
    # according to recommendations by SCOR WG 142 "Quality Control Procedures
    # for Oxygen and Other Biogeochemical Sensors on Floats and Gliders"
    #
    # Henry Bittig
    # Laboratoire d'Océanographie de Villefranche-sur-Mer, France
    # bittig@obs-vlfr.fr
    # 28.10.2015
    # 01.06.2022, Converted to python

    xO2 = 0.20946  # mole fraction of O2 in dry air (Glueckauf 1951)
    pH2Osat = 1013.25 * (
        np.exp(
            24.4543
            - (67.4509 * (100 / (T + 273.15)))
            - (4.8489 * np.log(((273.15 + T) / 100)))
            - 0.000544 * S
        )
    )  # saturated water vapor in mbar

    return O2sat / 100 * (xO2 * (p_atm - pH2Osat))
