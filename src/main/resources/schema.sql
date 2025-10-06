DROP TABLE IF EXISTS posts;
CREATE TABLE posts
(
    id         BIGSERIAL PRIMARY KEY,
    title      VARCHAR(255),
    content    VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO posts (title, content)
VALUES ('첫 번째 게시글', '이것은 첫 번째 게시글의 내용입니다.'),
       ('스프링 배치 소개', '스프링 배치는 대용량 데이터 처리를 위한 프레임워크입니다.'),
       ('JPA 학습하기', 'JPA는 자바 진영의 ORM 표준 기술입니다.'),
       ('데이터베이스 최적화', '데이터베이스 성능 향상을 위한 다양한 방법들을 알아봅시다.'),
       ('마이크로서비스 아키텍처', '마이크로서비스의 장단점과 구현 방법에 대해 설명합니다.'),
       ('도커 컨테이너 활용', '도커를 사용한 애플리케이션 배포 방법을 알아봅시다.'),
       ('테스트 코드 작성법', '효율적인 단위 테스트 작성 방법에 대해 설명합니다.'),
       ('시큐리티 구현하기', '스프링 시큐리티를 활용한 인증/인가 구현 방법을 소개합니다.'),
       ('REST API 설계', 'RESTful API 설계 원칙과 모범 사례를 알아봅시다.'),
       ('성능 모니터링', '애플리케이션 성능 모니터링 도구와 방법론을 소개합니다.');
