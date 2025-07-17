import pandas as pd

pd.options.mode.copy_on_write = True


def process_requests(requests: pd.DataFrame) -> pd.DataFrame:
    requests = requests.drop(
        # Unnecessary
        columns=["n"]
    ).rename(
        # Cleaner names
        columns={
            "cahashid": "id",
            "Edad": "edad",
            "Sexo": "sexo",
            "AP_CV": "ap_cv",
            "zapcardd0": "ap_cv1",
            "zapcardd1": "ap_cv2",
            "zapcardd2": "ap_cv3",
            "zapcardd3": "ap_cv4",
            "AP_FRCV": "ap_frcv",
            "Tabaco": "ap_tabaco",
            "HTA": "ap_hipertension_arterial",
            "Diabetes": "ap_diabetes",
            "Dislipemia": "ap_dislipemia",
            "Obesidad": "ap_obesidad",
            "eecg": "ecg_previo",
            "zeecgr0": "ecg_previo_resultado1",
            "zeecgr1": "ecg_previo_resultado2",
            "ehol": "holter_previo",
            "zeholr0": "holter_previo_resultado1",
            "zeholr1": "holter_previo_resultado2",
            "diag_1": "diagnostico1",
            "diag_2": "diagnostico2",
            "mdeo_int": "montevideo_interior",
        },
    )
    requests = requests[~requests["id"].isna()]

    for column in [
        "ap_cv",
        "ap_tabaco",
        "ap_diabetes",
        "ap_dislipemia",
        "ap_hipertension_arterial",
        "ap_obesidad",
        "ecg_previo",
        "holter_previo",
    ]:
        requests[column] = clean_bool(requests[column])

    return requests


def process_pacemakers(pacemakers: pd.DataFrame) -> pd.DataFrame:
    pacemakers = pacemakers.drop(
        # Unnecessary
        columns=["n"],
    ).rename(
        # Cleaner names
        columns={
            "cahashid": "id",
            "zcasinst": "imae_implante",
            "caorigen": "tipo_imae_implante",
            "oportunidad": "oportunidad_implante",
            "vivo": "vivo_al_alta",
            "vía_abordaje": "via_de_abordaje",
            "uso_intro": "uso_de_introductor",
            "modo": "modo_de_marcapasos",
            "comp": "complicaciones",
            "cafecrea": "fecha_realizacion",
            "año": "anio_realizacion",
            "cual_comp1": "complicacion1",
            "cual_comp2": "complicacion2",
        },
    )
    pacemakers = pacemakers[~pacemakers["id"].isna()]

    pacemakers["vivo_al_alta"] = pacemakers["vivo_al_alta"].map(
        {"Vivo": "S", "Fallecido": "N"}
    )

    pacemakers["tipo_imae_implante"] = pacemakers["tipo_imae_implante"].map(
        {"PRI": "privado", "PUBLICO": "publico"}
    )

    for column in ["uso_de_introductor", "complicaciones"]:
        pacemakers[column] = clean_bool(pacemakers[column])

    return pacemakers


def merge_dataframes(requests: pd.DataFrame, pacemakers: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(
        requests,
        pacemakers,
        on="id",
        how="inner",
        suffixes=(None, None),
    )


def process_merged(merged: pd.DataFrame) -> pd.DataFrame:
    merged = merged.drop_duplicates(subset=["id"])

    merged = merged[merged["sexo"].isin(["M", "F"]) & merged["edad"].notna()]

    merged["complicaciones"] = clean_bool(merged["complicaciones"])

    return merged


def clean_bool(series: pd.Series) -> pd.Series:
    return series.map({0: "N", 1: "S", pd.NA: "N", "S": "S", "N": "N"})


def clean() -> None:
    requests = pd.read_excel("requests.xls")
    pacemakers = pd.read_excel("pacemakers.xls", parse_dates=["cafecrea"])

    requests = process_requests(requests)
    pacemakers = process_pacemakers(pacemakers)

    merged = merge_dataframes(requests, pacemakers)
    merged = process_merged(merged)

    merged.to_csv("data.tsv", index=False, sep="\t")


if __name__ == "__main__":
    clean()
