import pandas as pd

pd.options.mode.copy_on_write = True


def process_requests(requests_df: pd.DataFrame) -> pd.DataFrame:
    requests_df = requests_df.drop(
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
            "Tabaco": "tabaco",
            "HTA": "hipertension_arterial",
            "Diabetes": "diabetes",
            "Dislipemia": "dislipemia",
            "Obesidad": "obesidad",
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

    for column in [
        "ap_cv",
        "tabaco",
        "diabetes",
        "dislipemia",
        "hipertension_arterial",
        "obesidad",
        "ecg_previo",
        "holter_previo",
    ]:
        requests_df[column] = clean_bool(requests_df[column])

    return requests_df


def process_pacemakers(pacemakers_df: pd.DataFrame) -> pd.DataFrame:
    pacemakers_df = pacemakers_df.drop(
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
            "cual_comp1": "complicacion1",
            "cual_comp2": "complicacion2",
        },
    )

    pacemakers_df["vivo_al_alta"] = pacemakers_df["vivo_al_alta"].map(
        {"Vivo": "S", "Fallecido": "N"}
    )

    pacemakers_df["tipo_imae_implante"] = pacemakers_df["tipo_imae_implante"].map(
        {"PRI": "privado", "PUBLICO": "publico"}
    )

    for column in ["uso_de_introductor", "complicaciones"]:
        pacemakers_df[column] = clean_bool(pacemakers_df[column])

    return pacemakers_df


def merge_dataframes(
    requests_df: pd.DataFrame, pacemakers_df: pd.DataFrame
) -> pd.DataFrame:
    return pd.merge(
        requests_df,
        pacemakers_df,
        on="id",
        how="inner",
        suffixes=(None, None),
    )


def process_merged(merged_df: pd.DataFrame) -> pd.DataFrame:
    merged_df = merged_df.drop_duplicates(subset=["id"])

    merged_df = merged_df[
        merged_df["sexo"].isin(["M", "F"]) & merged_df["edad"].notna()
    ]

    merged_df["complicaciones"] = clean_bool(merged_df["complicaciones"])

    return merged_df


def clean_bool(series: pd.Series) -> pd.Series:
    return series.map({0: "N", 1: "S", pd.NA: "N", "S": "S", "N": "N"})


def clean() -> None:
    requests_df = pd.read_excel("requests.xls")
    pacemakers_df = pd.read_excel("pacemakers.xls")

    requests_df = process_requests(requests_df)
    pacemakers_df = process_pacemakers(pacemakers_df)

    merged_df = merge_dataframes(requests_df, pacemakers_df)
    merged_df = process_merged(merged_df)

    merged_df.to_csv("data.tsv", index=False, sep="\t")


if __name__ == "__main__":
    clean()
