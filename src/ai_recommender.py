import os
from google import genai

def generate_ai_insights(top_genres=None, review_keywords=None):
    if top_genres is None:
        top_genres = ["Sci-Fi (40%)", "Action (35%)", "Comedy (25%)"]
    if review_keywords is None:
        review_keywords = ["plot twist", "buffering issue", "cinematography", "ending cliffhanger"]

    # Retrieve GEMINI_API_KEY from environment or fallback to hardcoded string
    api_key = os.environ.get("GEMINI_API_KEY", "AIzaSyB2ICLVwqLNjS2C8o7OM-ZMHDhz36Ut6l0")
    
    if api_key == "YOUR_ACTUAL_GEMINI_API_KEY" or not api_key:
        print("⚠️ Warning: Please set GEMINI_API_KEY environment variable or paste a valid key string.")

    client = genai.Client(api_key=api_key)

    prompt = f"""
    You are an AI Content Strategy Lead for an OTT streaming platform (like Netflix).
    Based on our weekly Big Data pipeline processing, here are the top platform metrics:
    - Top Performing Genres: {top_genres}
    - Most Frequent Review Keywords: {review_keywords}

    Please construct a brief, professional 2-paragraph executive report that:
    1. Summarizes viewer sentiment and content demand.
    2. Suggests 3 specific action points for new content acquisitions and system improvements.
    """

    print("\n🤖 Sending Big Data Metrics to Gemini API for GenAI Insights...")
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
        )
        print("\n--- 💡 Automated Executive AI Report ---")
        print(response.text)
        return response.text
    except Exception as e:
        print(f"⚠️ Gemini API Call Error: {e}")
        return None

if __name__ == "__main__":
    # Test call with sample metrics
    generate_ai_insights(
        top_genres=["Sci-Fi (40%)", "Action (35%)"],
        review_keywords=["plot twist", "buffering issue", "cinematography"]
    )
