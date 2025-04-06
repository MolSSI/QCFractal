import { useNavigate } from 'react-router-dom'


const HomePage = () => {
    const navigate = useNavigate();

    return (
        <>
            <h1>Profile</h1>
            <p>This is your profile page.</p>
            <button onClick={() => navigate('/projects/9')}>Go to Project 9</button>
            <button onClick={() => navigate('/projects/10')}>Go to Project 10</button>
            <button onClick={() => navigate('/projects/9/records/120382130')}>Example record</button>
        </>
    );
};

export default HomePage;