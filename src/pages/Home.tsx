import { useNavigate } from "react-router-dom";

const HomePage = () => {
  const navigate = useNavigate();

  return (
    <>
      <h1>Profile</h1>
      <p>This is your profile page.</p>
      <button onClick={() => navigate("/projects/11")}>Go to Project 11</button>
      <button onClick={() => navigate("/projects/12")}>Go to Project 12</button>
      <button onClick={() => navigate("/projects/11/records/120382130")}>
        Example record
      </button>
    </>
  );
};

export default HomePage;
