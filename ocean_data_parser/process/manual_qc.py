"""
QC Module present a set of tools to manually qc data.
"""
import logging

from ocean_data_parser.read import utils

from .process import xr

logger = logging.getLogger(__name__)

flag_conventions = {
    "QARTOD": {
        1: {
            "Meaning": "GOOD",
            "Description": "Data have passed critical real-time quality control tests and are deemed "
            "adequate for use as preliminary data.",
            "Value": 1,
            "Color": "#2ECC40",
        },
        2: {
            "Meaning": "UNKNOWN",
            "Description": "Data have not been QC-tested, or the information on quality is not available.",
            "Value": 2,
            "Color": "#FFDC00",
        },
        3: {
            "Meaning": "SUSPECT",
            "Description": "Data are considered to be either suspect or of high interest to data providers and users. "
            "They are flagged suspect to draw further attention to them by operators.",
            "Value": 3,
            "Color": "#FF851B",
        },
        4: {
            "Meaning": "FAIL",
            "Description": "Data are considered to have failed one or more critical real-time QC checks. "
            "If they are disseminated at all, it should be readily apparent that they "
            "are not of acceptable quality.",
            "Value": 4,
            "Color": "#FF4136",
        },
        9: {
            "Meaning": "MISSING",
            "Description": "Data are missing; used as a placeholder.",
            "Value": 9,
            "Color": "#85144b",
        },
    },
    "HAKAI": {
        "ADL": {
            "Description": "Value was above the established detection limit of the sensor",
            "Meaning": "Above detection limit",
            "Value": "ADL",
            "Color": "#2ECC40",
        },
        "AR": {
            "Description": "Value above a specified upper limit",
            "Meaning": "Above range",
            "Value": "AR",
            "Color": "#2ECC40",
        },
        "AV": {
            "Description": "Has been reviewed and looks good",
            "Meaning": "Accepted value",
            "Value": "AV",
            "Color": "#2ECC40",
        },
        "BDL": {
            "Description": "Value was below the established detection limit of the sensor",
            "Meaning": "Below detection limit",
            "Value": "BDL",
            "Color": "#00ffff",
        },
        "BR": {
            "Description": "Value below a specified lower limit",
            "Meaning": "Below range",
            "Value": "BR",
            "Color": "#2ECC40",
        },
        "CD": {
            "Description": "Sensor needs to be sent back to the manufacturer for calibration",
            "Meaning": "Calibration due",
            "Value": "CD",
            "Color": "#2ECC40",
        },
        "CE": {
            "Description": "Value was collected with a sensor that is past due for calibration",
            "Meaning": "Calibration expired",
            "Value": "CE",
            "Color": "#2ECC40",
        },
        "EV": {
            "Description": "Value has been estimated",
            "Meaning": "Estimated value",
            "Value": "EV",
            "Color": "#2ECC40",
        },
        "IC": {
            "Description": "One or more non‐sequential date/time values",
            "Meaning": "Invalid chronology",
            "Value": "IC",
            "Color": "#2ECC40",
        },
        "II": {
            "Description": "Value was inconsistent with another related measurement",
            "Meaning": "Internal inconsistency",
            "Value": "II",
            "Color": "#2ECC40",
        },
        "LB": {
            "Description": "Sensor battery dropped below a threshold",
            "Meaning": "Low battery",
            "Value": "LB",
            "Color": "#2ECC40",
        },
        "MV": {
            "Description": "No measured value available because of equipment failure, etc.",
            "Meaning": "Missing value",
            "Value": "MV",
            "Color": "#FFDC00",
        },
        "PV": {
            "Description": "Repeated value for an extended period",
            "Meaning": "Persistent value",
            "Value": "PV",
            "Color": "#2ECC40",
        },
        "SE": {
            "Description": "Value much greater than the previous value, resulting in an unrealistic slope",
            "Meaning": "Slope exceedance",
            "Value": "SE",
            "Color": "#2ECC40",
        },
        "SI": {
            "Description": "Value greatly differed from values collected from nearby sensors",
            "Meaning": "Spatial inconsistency",
            "Value": "SI",
            "Color": "#2ECC40",
        },
        "SVC": {
            "Description": "Value appears to be suspect, use with caution",
            "Meaning": "Suspicious value - caution",
            "Value": "SVC",
            "Color": "#FF851B",
        },
        "SVD": {
            "Description": "Value is clearly suspect, recommend discarding",
            "Meaning": "Suspicious value - reject",
            "Value": "SVD",
            "Color": "#FF4136",
        },
        "NaN": {
            "Description": "No value available",
            "Meaning": "Not available",
            "Value": "NaN",
            "Color": "#85144b",
        },
    },
    "priority": {
        "HAKAI": [
            "NaN",
            "MV",
            "AV",
            "CD",
            "CE",
            "IC",
            "LB",
            "ADL",
            "AD",
            "BDL",
            "EV",
            "BR",
            "II",
            "SI",
            "SE",
            "PV",
            "SVC",
            "SVD",
        ],
        "QARTOD": [9, 2, 1, 3, 4],
    },
    "mapping": {"QARTOD-HAKAI": {1: "AV", 2: "MV", 3: "SVC", 4: "SVD", 9: "NaN"}},
}


def get_manual_flag_attributes(convention, var=None):
    if isinstance(convention, str) and convention in flag_conventions:
        convention = flag_conventions[convention]

    return {
        "flag_meaning": " ".join([attrs["Meaning"] for attrs in convention.values()]),
        "flag_values": list(convention.keys()),
        "long_name": f"{var} Manual Flag" if var else "Manual Flag",
    }


def compare_flags(flags, convention=None, flag_priority=None):
    """
    General method that compare flags from the different flag conventions
    present in the flag_conventions dictionary by apply the priority list which is ordered from  the
    least to most prioritized flag.

    """
    if convention and convention in flag_conventions:
        flag_priority = flag_conventions["priority"][convention]

    record_flag = None
    for flag in flag_priority:
        if flag in flags:
            record_flag = flag
    return record_flag


def manual_qc_interface(
    ds,
    variable_list: list,
    convention: dict or str,
    manual_flag_suffix: str = "_review_flag",
    comment_column: str = "comment",
    default_flag=None,
    start_flag: str = None,
    agg_flag_method=None,
    netcdf_output_kwargs=None,
):
    """
    Manually QC interface to manually QC oceanographic data, through a Jupyter notebook.
    :param default_flag:
    :param comment_column:
    :param df: DataFrame input to QC
    :param variable_list: Variable List to review
    :param flags: Flag convention used
    :param review_flag:
    """

    try:
        import plotly.graph_objects as go
    except ImportError: 
        logger.error("Failed to import plotly")

    try:
        from IPython.display import display
    except ImportError: 
        logger.error('Failed to import IPython.')
    
    try:
        from ipywidgets import HBox, VBox, interactive, widgets
    except ImportError:
        logger.error("Failed to import ipywidgets.")

    # Convert dataset to dataframe
    df = ds.to_dataframe()
    index = df.index.name
    df = df.reset_index()

    # Retrieve Flag Convention
    if isinstance(convention, str) and convention in flag_conventions:
        convention = flag_conventions[convention]
        flag_options = [(item["Meaning"], key) for key, item in convention.items()]

    elif isinstance(convention, dict):
        flag_options = [(key, item) for key, item in convention.items()]
    else:
        raise TypeError(f"Unknown flag convention={convention}")

    # Set Widgets of the interface
    yaxis = widgets.Dropdown(
        options=variable_list,
        value=variable_list[0],
        description="Y Axis:",
        disabled=False,
    )

    xaxis = widgets.Dropdown(
        options=["depth", "time"],
        value="time",
        description="X Axis:",
        disabled=False,
    )

    filter_by = widgets.Text(
        value=None,
        description="Filter by",
        placeholder="ex: 20<depth<30",
        disabled=False,
    )

    filter_by_result = filter_by_result = widgets.HTML(
        value="{0} records available".format(len(df)),
    )

    flag_selection = widgets.Dropdown(
        options=flag_options,
        description="Apply Flag",
        disabled=False,
        value=start_flag,
    )
    flag_apply_to = widgets.SelectMultiple(
        options=variable_list,
        description="Apply flag to",
        disabled=False,
    )
    flag_comment = widgets.Textarea(
        value="",
        placeholder="Add review comment",
        description="Comment:",
        disabled=False,
    )

    apply_flag = widgets.Button(
        value=False,
        description="Apply",
        disabled=False,
        button_style="success",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip="Apply Flag to select records.",
    )

    accordion = widgets.Accordion()
    accordion.selected_index = None

    show_selection = widgets.Button(
        value=False,
        description="Show Selection",
        disabled=False,
        button_style="success",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip="Present selected records in table.",
    )
    save_button = widgets.Button(
        value=False,
        description="Save",
        disabled=False,
        button_style="success",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip="Save manually qced file to netcdf",
    )
    get_level1a = widgets.Checkbox(
        value=True, description="Level 1a (with flag)", indent=False
    )
    get_level1b = widgets.Checkbox(
        value=True, description="Level 1b (no flag)", indent=False
    )
    figure = go.FigureWidget(
        layout=go.Layout(
            barmode="overlay",
            margin=dict(l=50, r=20, t=50, b=20),
            xaxis=dict(title=xaxis.value),
            yaxis=dict(title=yaxis.value),
            height=400,
            title="Review",
            showlegend=True,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        ),
    )
    selected_table = widgets.Output()

    def get_filtered_data(df):
        """Apply query if available otherwise give back the full dataframe"""
        try:
            return df.query(filter_by.value) if filter_by.value else df, None
        except ValueError:
            return df, "Failed"

    def _get_plots():
        """Generate plots based on the dataframe df, yaxis and xaxis values present
        within the respective widgets and flags in seperate colors"""
        plots = []
        for flag_name, flag_value in convention.items():
            if type(flag_value) is dict and "Color" in flag_value:
                flag_color = flag_value["Color"]
                flag_meaning = flag_value["Meaning"]
            else:
                flag_color = flag_value
                flag_meaning = flag_value

            df_temp, error = get_filtered_data(df)
            manual_flag_column = yaxis.value + manual_flag_suffix
            if manual_flag_column not in df_temp:
                df_temp[manual_flag_column] = default_flag

            df_flag = df_temp.loc[df_temp[manual_flag_column] == flag_name]
            plots += [
                go.Scattergl(
                    x=df_flag[xaxis.value],
                    y=df_flag[yaxis.value],
                    mode="lines+markers" if flag_meaning in ("GOOD", 1) else "markers",
                    name=flag_meaning,
                    marker={"color": flag_color, "opacity": 1},
                )
            ]

        return tuple(plots)

    figure = go.FigureWidget(
        data=_get_plots(),
        layout=go.Layout(
            barmode="overlay",
            margin=dict(l=50, r=20, t=50, b=20),
            xaxis=dict(title=xaxis.value),
            yaxis=dict(title=yaxis.value),
            title="Review",
            showlegend=True,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
            dragmode="lasso",
            template="simple_white",
            height=1000,
        ),
    )
    figure.update_xaxes(mirror=True, ticks="outside", showline=True)
    figure.update_yaxes(mirror=True, ticks="outside", showline=True)

    def update_filter(query_string=None):
        """Update filter report below the filter_by cell"""
        df_temp, error = get_filtered_data(df)

        if error:
            filter_by_result.value = "<p style='color:red;'>Query failed!</p>"
        elif len(df_temp) == 0:
            # Give a message back saying no match and don't change anything else
            filter_by_result.value = "<p style='color:red;'>0 records found</p>"
        else:
            # Update text back and update plot with selection
            filter_by_result.value = "{0} records found".format(len(df_temp))

    def update_figure(_):
        """Update figure with present x and y items in menu"""
        update_axes(xaxis.value, yaxis.value)

    def update_axes(xvar, yvar):
        """
        Update figure, based on x,y axis provided
        :param xvar:
        :param yvar:
        """
        kk = 0
        with figure.batch_update():
            figure.layout.xaxis.title = xvar
            figure.layout.yaxis.title = yvar
            for plot in _get_plots():
                figure.data[kk].x = plot.x
                figure.data[kk].y = plot.y
                kk += 1
        update_flag_apply_to_selection(yvar)

    def _get_selected_records():
        """Method to retrieve the x and y coordinates of the records selected with the plotly lasso tool."""
        xs = []
        ys = []
        for layer in figure.data:
            if layer["selectedpoints"]:
                xs += list(layer.x[list(layer["selectedpoints"])])
                ys += list(layer.y[list(layer["selectedpoints"])])
        return xs, ys

    def _get_selected_indexes(xs, ys):
        """Method to retrieve dataframe indexes of the selected x,y records shown on the figure."""
        df_temp, error = get_filtered_data(df)
        is_indexes_selected = (
            df_temp[[xaxis.value, yaxis.value]]
            .apply(tuple, axis=1)
            .isin(tuple(zip(xs, ys)))
        )
        return df_temp.index[is_indexes_selected].tolist()

    def selection_fn(_):
        """Method to update the table showing the selected records."""
        xs, ys = _get_selected_records()
        selected_indexes = _get_selected_indexes(xs, ys)
        if selected_indexes:
            with selected_table:
                selected_table.clear_output()
                display(df.loc[selected_indexes])

    def save_to_netcdf(_):
        def _add_ancillary(ancillary, related):
            if "ancillary_variables" in ds_out[related].attrs:
                ds_out[related].attrs["ancillary_variables"] += f" {ancillary}"
            ds_out[related].attrs["ancillary_variables"] = ancillary

        # Retrieve daframe and convert it back xarray with added flag columns
        ds_out = df.set_index(index).to_xarray()
        ds_out.attrs = ds.attrs
        ds_out.process.filename_convention = ds.process.filename_convention
        for var in ds_out:
            if var in ds:
                ds_out[var].attrs = ds[var].attrs
            manual_flag_var = var + manual_flag_suffix
            if manual_flag_var not in ds_out:
                continue
            _add_ancillary(manual_flag_var, var)
            ds_out[manual_flag_var].attrs = get_manual_flag_attributes(convention, var)

        ds_out = utils.standardize_dataset(ds_out)
        # Save level1a
        if get_level1a.value:
            print("save levelA")
            ds_out.process.to_netcdf(suffix="_L1a_review", **netcdf_output_kwargs)

        if get_level1b.value:
            print("save levelB")
            ds_out = ds_out.process.drop_flagged_data(flags=[4], drop_flags=True)
            ds_out.process.filename_convention = ds.process.filename_convention
            ds_out.process.to_netcdf(suffix="_L1b_flagged", **netcdf_output_kwargs)

    def update_flag_apply_to_selection(yaxis_value):
        """Update variable to flag selection when one flag is
        previously selected and apply same as y axis"""
        selected_flag_variables = flag_apply_to.value
        if len(selected_flag_variables) <= 1 and yaxis_value in flag_apply_to.options:
            flag_apply_to.value = [yaxis_value]

    def update_flag_in_dataframe(_):
        """Tool triggered  when flag is applied to selected records."""

        def _generate_new_flag_from_ancillary_variables(var):
            # Verify if flag is available
            manual_flag = var + manual_flag_suffix
            if manual_flag in df:
                return
            # Get ancillary variables
            ancillary_variables = ds[var].attrs.get("ancillary_variables")
            if not ancillary_variables:
                df[manual_flag] = default_flag
                return
            ancillary_variables = ancillary_variables.split(" ")
            if len(ancillary_variables) == 1:
                df[manual_flag] = df[ancillary_variables[0]]
                return
            elif agg_flag_method is None:
                raise RuntimeError(
                    "Multiple ancillary_variables are associated to "
                    f"the variable {var} with {ancillary_variables=}, "
                    "please define an agg_flag_method"
                )

            df[manual_flag] = df[ancillary_variables].apply(
                agg_flag_method, axis="columns"
            )

        # Retrieve selected records and flag column
        xs, ys = _get_selected_records()
        selected_indexes = _get_selected_indexes(xs, ys)
        flag_names = [flag + manual_flag_suffix for flag in flag_apply_to.value]
        comment_name = yaxis.value + comment_column

        # Create a column for the manual flag if it doesn't exist
        for var in flag_apply_to.value:
            _generate_new_flag_from_ancillary_variables(var)

        # Print below the interface what's happening
        print(
            f"Apply {flag_selection.value} to {selected_indexes} records to {flag_names}",
            end="",
        )
        if flag_comment.value:
            print(f" and add comment: {flag_comment.value}", end="")
        print(" ... ", end="")

        # Update flag value within the data frame
        df.loc[selected_indexes, flag_names] = flag_selection.value

        # Update comment
        if flag_comment.value:
            df.loc[selected_indexes, comment_name] = flag_comment.value

        # Update figure with the new flags
        update_figure(True)
        print("Completed")

    # Setup the interaction between the different components
    axis_dropdowns = interactive(update_axes, yvar=yaxis, xvar=xaxis)
    show_selection.on_click(selection_fn)
    filter_by.on_submit(update_figure)
    apply_flag.on_click(update_flag_in_dataframe)
    interactive(update_filter, query_string=filter_by)
    save_button.on_click(save_to_netcdf)

    # Initialize
    update_figure(None)

    # Create the interface layout
    plot_interface = VBox(
        (*axis_dropdowns.children, filter_by, filter_by_result),
        layout={"align_items": "flex-end"},
    )
    flag_interface = HBox(
        (
            flag_apply_to,
            VBox(
                (flag_selection, flag_comment, apply_flag),
                layout={"align_items": "flex-end"},
            ),
        )
    )
    save_section = VBox((save_button, get_level1a, get_level1b))
    upper_menu = HBox(
        (
            HBox(
                (
                    plot_interface,
                    VBox(
                        (show_selection, save_section),
                        layout={"align_items": "stretch"},
                    ),
                )
            ),
            flag_interface,
        ),
        layout={"justify_content": "space-between"},
    )

    return VBox(
        (
            upper_menu,
            figure,
        )
    )
