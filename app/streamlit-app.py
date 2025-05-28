import streamlit as st
import pandas as pd
import pydeck as pdk
import googlemaps
from databricks import sql
import json

# Databricks and Google Maps configurations
DATABRICKS_CONFIG = st.secrets["databricks"]
GOOGLE_MAPS_API_KEY = st.secrets["google_maps"]["api_key"]


# Connect to Databricks SQL
@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_CONFIG["server_hostname"],
        http_path=DATABRICKS_CONFIG["http_path"],
        access_token=DATABRICKS_CONFIG["access_token"]
    )


# Initialize Google Maps client
gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

st.set_page_config(page_title="📍 Google Maps Route", layout="wide")
st.title("🗺️ Google Maps Route")

# Get connection
connection = get_connection()

# --- Select Order ID ---
try:
    order_df = pd.read_sql("SELECT DISTINCT order_id FROM ubereats.delivery.hub_order", connection)
    selected_order = st.selectbox("Select an Order ID", order_df["order_id"].tolist())
except Exception as e:
    st.error(f"Failed to fetch order IDs: {e}")
    st.stop()

# --- Display Route ---
if selected_order:
    route_query = f"""
    SELECT 
      s.start_lat,
      s.start_lon,
      s.end_lat,
      s.end_lon,
      s.distance_km,
      s.estimated_duration_min,
      s.start_time,
      s.end_time
    FROM ubereats.delivery.sat_route_metadata s
    JOIN ubereats.delivery.link_order_driver_route l
      ON s.hash_link_order_driver_route = l.hash_link_order_driver_route
    JOIN ubereats.delivery.hub_order h
      ON l.hash_hub_order_id = h.hash_hub_order_id
    WHERE h.order_id = '{selected_order}'
    """

    try:
        route_df = pd.read_sql(route_query, connection)

        if route_df.empty:
            st.warning("No route data found for the selected order.")
        else:
            st.subheader("📊 Route Metadata")
            st.dataframe(route_df)

            row = route_df.iloc[0]
            start_coords = (row["start_lat"], row["start_lon"])
            end_coords = (row["end_lat"], row["end_lon"])

            # Debug information
            with st.expander("🔍 Debug Information"):
                st.write(f"Start coordinates: {start_coords}")
                st.write(f"End coordinates: {end_coords}")

            # Call Google Maps Directions API with better error handling
            try:
                directions = gmaps.directions(
                    origin=start_coords,
                    destination=end_coords,
                    mode="driving"
                )

                # Debug: Show the raw API response
                with st.expander("🔍 Raw API Response"):
                    st.json(directions)

                if not directions:
                    st.error("No directions returned from Google Maps API")
                    st.info("Possible reasons: Invalid coordinates, no route available, or API issues")

                elif len(directions) > 0:
                    # The API returns a list, first element contains the route info
                    route_data = directions[0]

                    if "overview_polyline" in route_data and "points" in route_data["overview_polyline"]:
                        polyline = route_data["overview_polyline"]["points"]
                        decoded = googlemaps.convert.decode_polyline(polyline)

                        # Debug decoded points
                        with st.expander("🔍 Decoded Polyline Points"):
                            st.write(f"Number of points: {len(decoded)}")
                            st.write(f"First few points: {decoded[:3]}")

                        # Create list of coordinates for the path
                        path_coordinates = [[p["lng"], p["lat"]] for p in decoded]

                        # Create DataFrame for pydeck with proper structure
                        polyline_df = pd.DataFrame({
                            "path": [path_coordinates]  # Single row with all coordinates
                        })

                        # Create path layer
                        path_layer = pdk.Layer(
                            "PathLayer",
                            data=polyline_df,
                            get_path="path",
                            get_color=[255, 140, 0],  # Orange color for better visibility
                            width_scale=5,
                            width_min_pixels=3,
                            pickable=True,
                            auto_highlight=True
                        )

                        # Add markers for start and end points
                        points_df = pd.DataFrame({
                            "lat": [start_coords[0], end_coords[0]],
                            "lon": [start_coords[1], end_coords[1]],
                            "type": ["Start", "End"],
                            "color": [[0, 255, 0, 200], [255, 0, 0, 200]],
                            "radius": [150, 150]
                        })

                        scatter_layer = pdk.Layer(
                            "ScatterplotLayer",
                            data=points_df,
                            get_position=["lon", "lat"],
                            get_color="color",
                            get_radius="radius",
                            radius_min_pixels=10,
                            radius_max_pixels=100,
                            pickable=True,
                            auto_highlight=True
                        )

                        # Set view state
                        view_state = pdk.ViewState(
                            latitude=(start_coords[0] + end_coords[0]) / 2,
                            longitude=(start_coords[1] + end_coords[1]) / 2,
                            zoom=12,
                            pitch=0
                        )

                        st.subheader("🗺️ Google Route on Map")
                        st.pydeck_chart(pdk.Deck(
                            layers=[path_layer, scatter_layer],
                            initial_view_state=view_state,
                            tooltip={"text": "{type}"},
                            map_style="mapbox://styles/mapbox/dark-v10"  # Better visibility
                        ))

                        # Show route summary
                        if "legs" in route_data:
                            leg = route_data["legs"][0]
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Distance", leg["distance"]["text"])
                            with col2:
                                st.metric("Duration", leg["duration"]["text"])

                            # Show driving steps
                            st.subheader("📍 Driving Steps")
                            if "steps" in leg:
                                for i, step in enumerate(leg["steps"], 1):
                                    # Clean HTML tags from instructions
                                    instruction = step.get("html_instructions", "").replace("<b>", "**").replace("</b>",
                                                                                                                 "**")
                                    instruction = instruction.replace("<div style=\"font-size:0.9em\">", " - ").replace(
                                        "</div>", "")

                                    st.markdown(
                                        f"{i}. {instruction} "
                                        f"({step['distance']['text']}, {step['duration']['text']})",
                                        unsafe_allow_html=False
                                    )
                    else:
                        st.error("No polyline data in the route response")
                else:
                    st.error("Unexpected API response structure")
                    st.json(directions)

            except googlemaps.exceptions.ApiError as e:
                st.error(f"Google Maps API Error: {e}")
                st.info("Check your API key and ensure it has the Directions API enabled")
            except Exception as e:
                st.error(f"Error calling Google Maps API: {e}")
                st.info("Check the debug information above for more details")

    except Exception as e:
        st.error(f"Error retrieving route data: {e}")
        import traceback

        with st.expander("🔍 Full Error Traceback"):
            st.code(traceback.format_exc())

# Note: Connection is managed by @st.cache_resource, no need to manually close
